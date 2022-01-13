package hls

import (
	"bytes"
	"container/ring"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	. "github.com/Monibuca/engine/v3"
	"github.com/Monibuca/utils/v3"
	"github.com/Monibuca/utils/v3/codec"
	"github.com/Monibuca/utils/v3/codec/mpegts"
)

var memoryTs sync.Map
var memoryM3u8 sync.Map
var tsFileBase string

func writeHLS(r *Stream) {
	if filterReg != nil && !filterReg.MatchString(r.StreamPath) {
		return
	}
	var m3u8Buffer bytes.Buffer
	var infoRing = ring.New(config.Window)
	for i := 0; i < infoRing.Len(); i++ {
		inf := PlaylistInf{
			Duration: 0,
		}
		infoRing.Value = inf
		infoRing = infoRing.Next()
	}

	memoryM3u8.Store(r.StreamPath, &m3u8Buffer)
	defer memoryM3u8.Delete(r.StreamPath)
	var err error
	var hls_fragment int64       // hls fragment
	var hls_segment_count uint32 // hls segment count
	var vwrite_time uint32
	var hls_segment_slice_count uint32 // hls segment count
	var vwrite_slice_time uint32
	var video_cc, audio_cc uint16
	var outStream = Subscriber{ID: "HLSWriter", Type: "HLS"}

	if err = outStream.Subscribe(r.StreamPath); err != nil {
		utils.Println(err)
		return
	}
	vt := outStream.WaitVideoTrack("h264")
	at := outStream.WaitAudioTrack("aac")
	if err != nil {
		return
	}
	var asc codec.AudioSpecificConfig
	if at != nil {
		asc, err = decodeAudioSpecificConfig(at.ExtraData)
	}
	if err != nil {
		return
	}
	if config.Fragment > 0 {
		hls_fragment = config.Fragment * 1000
	} else {
		hls_fragment = 10000
	}

	hls_path := filepath.Join(config.Path, r.StreamPath, fmt.Sprintf("%d.m3u8", time.Now().Unix()))
	os.MkdirAll(filepath.Dir(hls_path), 0755)
	var file *os.File
	file, err = os.OpenFile(hls_path, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return
	}
	defer file.Close()
	hls_playlist := Playlist{
		Writer:          file,
		Version:         6,
		Sequence:        0,
		Targetduration:  int(hls_fragment / 666), // hlsFragment * 1.5 / 1000
		Segmentduration: (float32)(hls_fragment) / 2000,
	}
	if err = hls_playlist.Init(); err != nil {
		return
	}
	hls_segment_data := &bytes.Buffer{}
	hls_segment_slice_data := &bytes.Buffer{}
	outStream.OnVideo = func(ts uint32, pack *VideoPack) {
		packet, err := VideoPacketToPES(ts, pack, vt.ExtraData.NALUs)
		if err != nil {
			return
		}

		var update = false

		// 当前的时间戳减去上一个ts切片的时间戳
		if int64(ts-vwrite_slice_time) >= int64(hls_playlist.Segmentduration*1000) || pack.IDR {
			//fmt.Println("time :", video.Timestampayy, tsSegmentTimestamp)

			tsSliceFilename := tsFileBase + "." + strconv.FormatUint(uint64(hls_segment_slice_count), 10) + ".ts"

			tsData := hls_segment_slice_data.Bytes()
			tsFilePath := filepath.Join(filepath.Dir(hls_path), tsSliceFilename)

			if config.EnableWrite && len(tsData) > 0 {
				if err = writeHlsTsSegmentFile(tsFilePath, tsData); err != nil {
					return
				}
			}

			segInf := SegmentInf{
				Duration: float64(ts-vwrite_slice_time) / 1000.0,
				Title:    tsSliceFilename,
			}
			/*
				if pack.IDR {
					segInf.Independent = true
				}
			*/
			if !strings.Contains(infoRing.Value.(PlaylistInf).FilePath, tsFileBase) {
				inf := PlaylistInf{
					//浮点计算精度
					Duration:    0,
					FilePath:    tsFileBase + ".ts",
					ProgramTime: time.Now().UTC().Format("2006-01-02T15:04:05.000+00:00"),
				}
				infoRing.Value = inf
				segInf.Independent = true
			}
			inf := infoRing.Value.(PlaylistInf)
			inf.Segs = append(inf.Segs, segInf)
			infoRing.Value = inf

			hls_segment_slice_count++
			vwrite_slice_time = ts
			hls_segment_slice_data.Reset()
			update = true
		}

		if pack.IDR {
			// 当前的时间戳减去上一个ts切片的时间戳
			if int64(ts-vwrite_time) >= hls_fragment {
				//fmt.Println("time :", video.Timestamp, tsSegmentTimestamp)

				//tsFilename := strconv.FormatInt(time.Now().Unix(), 10) + ".ts"
				tsFilename := tsFileBase + ".ts"

				tsData := hls_segment_data.Bytes()
				tsFilePath := filepath.Join(filepath.Dir(hls_path), tsFilename)
				if config.EnableWrite {
					if err = writeHlsTsSegmentFile(tsFilePath, tsData); err != nil {
						return
					}
				}
				if config.EnableMemory {
					memoryTs.Store(tsFilePath, tsData)
				}

				if hls_segment_count >= uint32(config.Window) {
					m3u8Buffer.Reset()
					memoryTs.Delete(infoRing.Value.(PlaylistInf).FilePath)
				}
				inf := infoRing.Value.(PlaylistInf)
				inf.Title = tsFilename
				inf.Duration = float64((ts - vwrite_time) / 1000.0)
				infoRing.Value = inf

				//Switch next ring
				infoRing = infoRing.Next()

				hls_playlist.Targetduration = int(math.Ceil(float64(ts-vwrite_time) / 1000))

				hls_segment_count++
				vwrite_time = ts
				hls_segment_data.Reset()
				hls_segment_slice_count = 0

				update = true
			}
			tsFileBase = strconv.FormatInt(time.Now().Unix(), 10)
		}

		if update {
			os.Truncate(hls_path, 0)
			file.Seek(0, 0)

			//Write to m3u8
			if err = hls_playlist.Init(); err != nil {
				return
			}

			printRing := infoRing
			if infoRing.Value.(PlaylistInf).Duration == 0 {
				printRing = printRing.Next()
			} else {
				//Add sequence
				hls_playlist.Sequence++
			}

			printRing.Do(func(i interface{}) {
				hls_playlist.WriteInf(i.(PlaylistInf))
			})
		}

		frame := new(mpegts.MpegtsPESFrame)
		frame.Pid = 0x101
		frame.IsKeyFrame = pack.IDR
		frame.ContinuityCounter = byte(video_cc % 16)
		frame.ProgramClockReferenceBase = uint64(ts) * 90
		if err = mpegts.WritePESPacket(hls_segment_data, frame, packet); err != nil {
			return
		}

		frame = new(mpegts.MpegtsPESFrame)
		frame.Pid = 0x101
		frame.IsKeyFrame = true
		frame.ContinuityCounter = byte(video_cc % 16)
		frame.ProgramClockReferenceBase = uint64(ts) * 90
		if err = mpegts.WritePESPacket(hls_segment_slice_data, frame, packet); err != nil {
			return
		}

		video_cc = uint16(frame.ContinuityCounter)
	}
	outStream.OnAudio = func(ts uint32, pack *AudioPack) {
		var packet mpegts.MpegTsPESPacket
		if packet, err = AudioPacketToPES(ts, pack.Raw, asc); err != nil {
			return
		}

		frame := new(mpegts.MpegtsPESFrame)
		frame.Pid = 0x102
		frame.IsKeyFrame = false
		frame.ContinuityCounter = byte(audio_cc % 16)
		//frame.ProgramClockReferenceBase = 0
		if err = mpegts.WritePESPacket(hls_segment_data, frame, packet); err != nil {
			return
		}

		frame = new(mpegts.MpegtsPESFrame)
		frame.Pid = 0x102
		frame.IsKeyFrame = false
		frame.ContinuityCounter = byte(audio_cc % 16)
		//frame.ProgramClockReferenceBase = 0
		if err = mpegts.WritePESPacket(hls_segment_slice_data, frame, packet); err != nil {
			return
		}

		audio_cc = uint16(frame.ContinuityCounter)
	}
	outStream.Play(at, vt)

	if config.EnableMemory {
		infoRing.Do(func(i interface{}) {
			if i != nil {
				memoryTs.Delete(i.(PlaylistInf).FilePath)
			}
		})
	}
}
