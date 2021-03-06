package hls

import (
	"bytes"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	. "github.com/Monibuca/engine/v2"
	"github.com/Monibuca/engine/v2/avformat"
	"github.com/Monibuca/engine/v2/avformat/mpegts"
)

var memoryTs sync.Map

func writeHLS(r *Stream) {
	var avc avformat.AVCDecoderConfigurationRecord // AVCDecoderConfigurationRecord(mpegts)
	var asc avformat.AudioSpecificConfig           // AudioSpecificConfig(mpegts)
	var hls_path string                            // hls ts file path
	var hls_m3u8_name string                       // hls m3u8 name
	var hls_playlist Playlist                      // hls play list
	var hls_fragment int64                         // hls fragment
	var hls_segment_count uint32                   // hls segment count
	var hls_segment_data *bytes.Buffer             // hls segment
	var vwrite_time uint32
	var atwrite bool
	var video_cc, audio_cc uint16
	var outStream Subscriber
	var ring = NewRing(4)
	outStream.Type = "HLS"
	outStream.ID = "HLSWriter"
	sendHandler := func(p *avformat.SendPacket) (err error) {
		var packet mpegts.MpegTsPESPacket
		if p.Type == avformat.FLV_TAG_TYPE_VIDEO {
			if packet, err = rtmpVideoPacketToPES(p, avc); err != nil {
				return
			}
			video := p
			if video.IsKeyFrame {
				// 当前的时间戳减去上一个ts切片的时间戳
				if int64(p.Timestamp-vwrite_time) >= hls_fragment {
					//fmt.Println("time :", video.Timestamp, tsSegmentTimestamp)

					tsFilename := strconv.FormatInt(time.Now().Unix(), 10) + ".ts"

					tsData := hls_segment_data.Bytes()
					tsFilePath := filepath.Join(hls_path, tsFilename)
					if config.EnableWrite {
						if err = writeHlsTsSegmentFile(tsFilePath, tsData); err != nil {
							return
						}
					}
					if config.EnableMemory {
						ring.GetBuffer().Write(tsData)
						ring.Payload = []byte(tsFilePath)
						memoryTs.Store(tsFilePath, ring.RingItem)
						if ring.NextW(); len(ring.Payload) > 0 {
							memoryTs.Delete(string(ring.Payload))
						}
					}
					inf := PlaylistInf{
						Duration: float64((video.Timestamp - vwrite_time) / 1000),
						Title:    filepath.Base(hls_path) + "/" + tsFilename,
					}

					if hls_segment_count >= uint32(config.Window) {
						if err = hls_playlist.UpdateInf(hls_m3u8_name, hls_m3u8_name+".tmp", inf); err != nil {
							return
						}
					} else {
						if err = hls_playlist.WriteInf(hls_m3u8_name, inf); err != nil {
							return
						}
					}

					hls_segment_count++
					vwrite_time = p.Timestamp
					hls_segment_data.Reset()
				}
			}

			frame := new(mpegts.MpegtsPESFrame)
			frame.Pid = 0x101
			frame.IsKeyFrame = video.IsKeyFrame
			frame.ContinuityCounter = byte(video_cc % 16)
			frame.ProgramClockReferenceBase = uint64(video.Timestamp) * 90
			if err = mpegts.WritePESPacket(hls_segment_data, frame, packet); err != nil {
				return
			}

			video_cc = uint16(frame.ContinuityCounter)
		} else if p.Type == avformat.FLV_TAG_TYPE_AUDIO {
			if atwrite {
				var packet mpegts.MpegTsPESPacket
				if packet, err = rtmpAudioPacketToPES(p, asc); err != nil {
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

				audio_cc = uint16(frame.ContinuityCounter)

				return
			}

			if asc, err = decodeAudioSpecificConfig(p.AVPacket); err != nil {
				return
			}

			atwrite = true
		}
		return
	}
	outStream.OnData = func(packet *avformat.SendPacket) (err error) {
		if *outStream.EnableVideo && packet.Type == avformat.FLV_TAG_TYPE_AUDIO {
			return nil
		}
		if avc, err = decodeAVCDecoderConfigurationRecord(packet); err != nil {
			return
		}

		if config.Fragment > 0 {
			hls_fragment = config.Fragment * 1000
		} else {
			hls_fragment = 10000
		}

		hls_playlist = Playlist{
			Version:        3,
			Sequence:       0,
			Targetduration: int(hls_fragment / 666), // hlsFragment * 1.5 / 1000
		}

		hls_path = filepath.Join(config.Path, r.StreamPath)
		hls_m3u8_name = hls_path + ".m3u8"
		os.MkdirAll(hls_path, 0755)
		if err = hls_playlist.Init(hls_m3u8_name); err != nil {
			log.Println(err)
			return
		}

		hls_segment_data = &bytes.Buffer{}
		hls_segment_count = 0
		outStream.OnData = sendHandler
		return
	}
	if config.EnableMemory {
		go func() {
			if err := outStream.Subscribe(r.StreamPath); err != nil {
				Println(err)
			}
			for i := 0; i < ring.Size; i++ {
				memoryTs.Delete(string(ring.GetAt(i).Payload))
			}
		}()
	} else {
		go outStream.Subscribe(r.StreamPath)
	}
}
