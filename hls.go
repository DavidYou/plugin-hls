package hls

import (
	"fmt"
	"io"
	"strings"

	"github.com/Monibuca/utils/v3"
)

const (
	HLS_KEY_METHOD_AES_128 = "AES-128"
)

// https://datatracker.ietf.org/doc/draft-pantos-http-live-streaming/

// 以”#EXT“开头的表示一个”tag“,否则表示注释,直接忽略
type Playlist struct {
	io.Writer
	ExtM3U          string      // indicates that the file is an Extended M3U [M3U] Playlist file. (4.3.3.1) -- 每个M3U文件第一行必须是这个tag.
	Version         int         // indicates the compatibility version of the Playlist file. (4.3.1.2) -- 协议版本号.
	Sequence        int         // indicates the Media Sequence Number of the first Media Segment that appears in a Playlist file. (4.3.3.2) -- 第一个媒体段的序列号.
	Targetduration  int         // specifies the maximum Media Segment duration. (4.3.3.1) -- 每个视频分段最大的时长(单位秒).
	Segmentduration float32     // specifies the segment duration
	PlaylistType    int         // rovides mutability information about the Media Playlist file. (4.3.3.5) -- 提供关于PlayList的可变性的信息.
	Discontinuity   int         // indicates a discontinuity between theMedia Segment that follows it and the one that preceded it. (4.3.2.3) -- 该标签后边的媒体文件和之前的媒体文件之间的编码不连贯(即发生改变)(场景用于插播广告等等).
	Key             PlaylistKey // specifies how to decrypt them. (4.3.2.4) -- 解密媒体文件的必要信息(表示怎么对media segments进行解码).
	EndList         string      // indicates that no more Media Segments will be added to the Media Playlist file. (4.3.3.4) -- 标示没有更多媒体文件将会加入到播放列表中,它可能会出现在播放列表文件的任何地方,但是不能出现两次或以上.
	Inf             PlaylistInf // specifies the duration of a Media Segment. (4.3.2.1) -- 指定每个媒体段(ts)的持续时间.
}

// Discontinuity :
// file format
// number, type and identifiers of tracks
// timestamp sequence
// encoding parameters
// encoding sequence

type PlaylistKey struct {
	Method string // specifies the encryption method. (4.3.2.4)
	Uri    string // key url. (4.3.2.4)
	IV     string // key iv. (4.3.2.4)
}

type SegmentInf struct {
	Duration    float64
	Title       string
	Independent bool
}

type PlaylistInf struct {
	Duration    float64
	Title       string
	FilePath    string
	Segs        []SegmentInf
	ProgramTime string
}

func (pl *Playlist) Init() (err error) {
	// ss := fmt.Sprintf("#EXTM3U\n"+
	// 	"#EXT-X-VERSION:%d\n"+
	// 	"#EXT-X-MEDIA-SEQUENCE:%d\n"+
	// 	"#EXT-X-TARGETDURATION:%d\n"+
	// 	"#EXT-X-PLAYLIST-TYPE:%d\n"+
	// 	"#EXT-X-DISCONTINUITY:%d\n"+
	// 	"#EXT-X-KEY:METHOD=%s,URI=%s,IV=%s\n"+
	// 	"#EXT-X-ENDLIST", hls.Version, hls.Sequence, hls.Targetduration, hls.PlaylistType, hls.Discontinuity, hls.Key.Method, hls.Key.Uri, hls.Key.IV)

	ss := fmt.Sprintf("#EXTM3U\n"+
		"#EXT-X-VERSION:%d\n"+
		"#EXT-X-MEDIA-SEQUENCE:%d\n"+
		"#EXT-X-TARGETDURATION:%d\n"+
		"#EXT-X-INDEPENDENT-SEGMENTS\n"+
		"#EXT-X-SERVER-CONTROL:PART-HOLD-BACK=%.3f\n"+
		"#EXT-X-PART-INF:PART-TARGET=%.3f\n",
		pl.Version, pl.Sequence, pl.Targetduration,
		float32(3*pl.Segmentduration)+0.1, float32(pl.Segmentduration))

	_, err = pl.Write([]byte(ss))
	return
}

func (pl *Playlist) WriteInf(inf PlaylistInf) (err error) {
	for i := 0; i < len(inf.Segs); i++ {
		if len(inf.ProgramTime) != 0 && i == 0 {
			ss := fmt.Sprintf("#EXT-X-PROGRAM-DATE-TIME:%s\n", inf.ProgramTime)
			_, err = pl.Write([]byte(ss))
		}
		pl.WriteInfN(inf.Segs[i])
	}

	if inf.Duration != 0 {
		ss := fmt.Sprintf("#EXTINF:%.3f,\n%s\n", inf.Duration, inf.Title)
		_, err = pl.Write([]byte(ss))
	}
	return
}

func (pl *Playlist) WriteInfN(inf SegmentInf) (err error) {
	ss := fmt.Sprintf("#EXT-X-PART:URI=\"%s\",DURATION=%.3f", inf.Title, inf.Duration)
	if inf.Independent {
		ss += ",INDEPENDENT=YES"
	}
	ss += "\n"
	_, err = pl.Write([]byte(ss))
	return
}

func (pl *Playlist) GetInfCount(filename string) (num int, err error) {
	var ls []string
	if ls, err = utils.ReadFileLines(filename); err != nil {
		return
	}

	num = 0
	for _, v := range ls {
		if strings.Contains(v, "#EXTINF") {
			num++
		}
	}

	return
}
