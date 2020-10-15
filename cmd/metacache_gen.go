package cmd

// Code generated by github.com/tinylib/msgp DO NOT EDIT.

import (
	"github.com/tinylib/msgp/msgp"
)

// DecodeMsg implements msgp.Decodable
func (z *bucketMetacache) DecodeMsg(dc *msgp.Reader) (err error) {
	var field []byte
	_ = field
	var zb0001 uint32
	zb0001, err = dc.ReadMapHeader()
	if err != nil {
		err = msgp.WrapError(err)
		return
	}
	for zb0001 > 0 {
		zb0001--
		field, err = dc.ReadMapKeyPtr()
		if err != nil {
			err = msgp.WrapError(err)
			return
		}
		switch msgp.UnsafeString(field) {
		case "bucket":
			z.bucket, err = dc.ReadString()
			if err != nil {
				err = msgp.WrapError(err, "bucket")
				return
			}
		case "caches":
			var zb0002 uint32
			zb0002, err = dc.ReadMapHeader()
			if err != nil {
				err = msgp.WrapError(err, "caches")
				return
			}
			if z.caches == nil {
				z.caches = make(map[string]metacache, zb0002)
			} else if len(z.caches) > 0 {
				for key := range z.caches {
					delete(z.caches, key)
				}
			}
			for zb0002 > 0 {
				zb0002--
				var za0001 string
				var za0002 metacache
				za0001, err = dc.ReadString()
				if err != nil {
					err = msgp.WrapError(err, "caches")
					return
				}
				err = za0002.DecodeMsg(dc)
				if err != nil {
					err = msgp.WrapError(err, "caches", za0001)
					return
				}
				z.caches[za0001] = za0002
			}
		default:
			err = dc.Skip()
			if err != nil {
				err = msgp.WrapError(err)
				return
			}
		}
	}
	return
}

// EncodeMsg implements msgp.Encodable
func (z *bucketMetacache) EncodeMsg(en *msgp.Writer) (err error) {
	// map header, size 2
	// write "bucket"
	err = en.Append(0x82, 0xa6, 0x62, 0x75, 0x63, 0x6b, 0x65, 0x74)
	if err != nil {
		return
	}
	err = en.WriteString(z.bucket)
	if err != nil {
		err = msgp.WrapError(err, "bucket")
		return
	}
	// write "caches"
	err = en.Append(0xa6, 0x63, 0x61, 0x63, 0x68, 0x65, 0x73)
	if err != nil {
		return
	}
	err = en.WriteMapHeader(uint32(len(z.caches)))
	if err != nil {
		err = msgp.WrapError(err, "caches")
		return
	}
	for za0001, za0002 := range z.caches {
		err = en.WriteString(za0001)
		if err != nil {
			err = msgp.WrapError(err, "caches")
			return
		}
		err = za0002.EncodeMsg(en)
		if err != nil {
			err = msgp.WrapError(err, "caches", za0001)
			return
		}
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z *bucketMetacache) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// map header, size 2
	// string "bucket"
	o = append(o, 0x82, 0xa6, 0x62, 0x75, 0x63, 0x6b, 0x65, 0x74)
	o = msgp.AppendString(o, z.bucket)
	// string "caches"
	o = append(o, 0xa6, 0x63, 0x61, 0x63, 0x68, 0x65, 0x73)
	o = msgp.AppendMapHeader(o, uint32(len(z.caches)))
	for za0001, za0002 := range z.caches {
		o = msgp.AppendString(o, za0001)
		o, err = za0002.MarshalMsg(o)
		if err != nil {
			err = msgp.WrapError(err, "caches", za0001)
			return
		}
	}
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *bucketMetacache) UnmarshalMsg(bts []byte) (o []byte, err error) {
	var field []byte
	_ = field
	var zb0001 uint32
	zb0001, bts, err = msgp.ReadMapHeaderBytes(bts)
	if err != nil {
		err = msgp.WrapError(err)
		return
	}
	for zb0001 > 0 {
		zb0001--
		field, bts, err = msgp.ReadMapKeyZC(bts)
		if err != nil {
			err = msgp.WrapError(err)
			return
		}
		switch msgp.UnsafeString(field) {
		case "bucket":
			z.bucket, bts, err = msgp.ReadStringBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "bucket")
				return
			}
		case "caches":
			var zb0002 uint32
			zb0002, bts, err = msgp.ReadMapHeaderBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "caches")
				return
			}
			if z.caches == nil {
				z.caches = make(map[string]metacache, zb0002)
			} else if len(z.caches) > 0 {
				for key := range z.caches {
					delete(z.caches, key)
				}
			}
			for zb0002 > 0 {
				var za0001 string
				var za0002 metacache
				zb0002--
				za0001, bts, err = msgp.ReadStringBytes(bts)
				if err != nil {
					err = msgp.WrapError(err, "caches")
					return
				}
				bts, err = za0002.UnmarshalMsg(bts)
				if err != nil {
					err = msgp.WrapError(err, "caches", za0001)
					return
				}
				z.caches[za0001] = za0002
			}
		default:
			bts, err = msgp.Skip(bts)
			if err != nil {
				err = msgp.WrapError(err)
				return
			}
		}
	}
	o = bts
	return
}

// Msgsize returns an upper bound estimate of the number of bytes occupied by the serialized message
func (z *bucketMetacache) Msgsize() (s int) {
	s = 1 + 7 + msgp.StringPrefixSize + len(z.bucket) + 7 + msgp.MapHeaderSize
	if z.caches != nil {
		for za0001, za0002 := range z.caches {
			_ = za0002
			s += msgp.StringPrefixSize + len(za0001) + za0002.Msgsize()
		}
	}
	return
}

// DecodeMsg implements msgp.Decodable
func (z *metacache) DecodeMsg(dc *msgp.Reader) (err error) {
	var field []byte
	_ = field
	var zb0001 uint32
	zb0001, err = dc.ReadMapHeader()
	if err != nil {
		err = msgp.WrapError(err)
		return
	}
	for zb0001 > 0 {
		zb0001--
		field, err = dc.ReadMapKeyPtr()
		if err != nil {
			err = msgp.WrapError(err)
			return
		}
		switch msgp.UnsafeString(field) {
		case "id":
			z.id, err = dc.ReadString()
			if err != nil {
				err = msgp.WrapError(err, "id")
				return
			}
		case "b":
			z.bucket, err = dc.ReadString()
			if err != nil {
				err = msgp.WrapError(err, "bucket")
				return
			}
		case "root":
			z.root, err = dc.ReadString()
			if err != nil {
				err = msgp.WrapError(err, "root")
				return
			}
		case "rec":
			z.recursive, err = dc.ReadBool()
			if err != nil {
				err = msgp.WrapError(err, "recursive")
				return
			}
		case "stat":
			{
				var zb0002 uint8
				zb0002, err = dc.ReadUint8()
				if err != nil {
					err = msgp.WrapError(err, "status")
					return
				}
				z.status = scanStatus(zb0002)
			}
		case "fnf":
			z.fileNotFound, err = dc.ReadBool()
			if err != nil {
				err = msgp.WrapError(err, "fileNotFound")
				return
			}
		case "err":
			z.error, err = dc.ReadString()
			if err != nil {
				err = msgp.WrapError(err, "error")
				return
			}
		case "st":
			z.started, err = dc.ReadTime()
			if err != nil {
				err = msgp.WrapError(err, "started")
				return
			}
		case "end":
			z.ended, err = dc.ReadTime()
			if err != nil {
				err = msgp.WrapError(err, "ended")
				return
			}
		case "u":
			z.lastUpdate, err = dc.ReadTime()
			if err != nil {
				err = msgp.WrapError(err, "lastUpdate")
				return
			}
		case "lh":
			z.lastHandout, err = dc.ReadTime()
			if err != nil {
				err = msgp.WrapError(err, "lastHandout")
				return
			}
		case "stc":
			z.startedCycle, err = dc.ReadUint64()
			if err != nil {
				err = msgp.WrapError(err, "startedCycle")
				return
			}
		case "endc":
			z.endedCycle, err = dc.ReadUint64()
			if err != nil {
				err = msgp.WrapError(err, "endedCycle")
				return
			}
		case "v":
			z.dataVersion, err = dc.ReadUint8()
			if err != nil {
				err = msgp.WrapError(err, "dataVersion")
				return
			}
		default:
			err = dc.Skip()
			if err != nil {
				err = msgp.WrapError(err)
				return
			}
		}
	}
	return
}

// EncodeMsg implements msgp.Encodable
func (z *metacache) EncodeMsg(en *msgp.Writer) (err error) {
	// map header, size 14
	// write "id"
	err = en.Append(0x8e, 0xa2, 0x69, 0x64)
	if err != nil {
		return
	}
	err = en.WriteString(z.id)
	if err != nil {
		err = msgp.WrapError(err, "id")
		return
	}
	// write "b"
	err = en.Append(0xa1, 0x62)
	if err != nil {
		return
	}
	err = en.WriteString(z.bucket)
	if err != nil {
		err = msgp.WrapError(err, "bucket")
		return
	}
	// write "root"
	err = en.Append(0xa4, 0x72, 0x6f, 0x6f, 0x74)
	if err != nil {
		return
	}
	err = en.WriteString(z.root)
	if err != nil {
		err = msgp.WrapError(err, "root")
		return
	}
	// write "rec"
	err = en.Append(0xa3, 0x72, 0x65, 0x63)
	if err != nil {
		return
	}
	err = en.WriteBool(z.recursive)
	if err != nil {
		err = msgp.WrapError(err, "recursive")
		return
	}
	// write "stat"
	err = en.Append(0xa4, 0x73, 0x74, 0x61, 0x74)
	if err != nil {
		return
	}
	err = en.WriteUint8(uint8(z.status))
	if err != nil {
		err = msgp.WrapError(err, "status")
		return
	}
	// write "fnf"
	err = en.Append(0xa3, 0x66, 0x6e, 0x66)
	if err != nil {
		return
	}
	err = en.WriteBool(z.fileNotFound)
	if err != nil {
		err = msgp.WrapError(err, "fileNotFound")
		return
	}
	// write "err"
	err = en.Append(0xa3, 0x65, 0x72, 0x72)
	if err != nil {
		return
	}
	err = en.WriteString(z.error)
	if err != nil {
		err = msgp.WrapError(err, "error")
		return
	}
	// write "st"
	err = en.Append(0xa2, 0x73, 0x74)
	if err != nil {
		return
	}
	err = en.WriteTime(z.started)
	if err != nil {
		err = msgp.WrapError(err, "started")
		return
	}
	// write "end"
	err = en.Append(0xa3, 0x65, 0x6e, 0x64)
	if err != nil {
		return
	}
	err = en.WriteTime(z.ended)
	if err != nil {
		err = msgp.WrapError(err, "ended")
		return
	}
	// write "u"
	err = en.Append(0xa1, 0x75)
	if err != nil {
		return
	}
	err = en.WriteTime(z.lastUpdate)
	if err != nil {
		err = msgp.WrapError(err, "lastUpdate")
		return
	}
	// write "lh"
	err = en.Append(0xa2, 0x6c, 0x68)
	if err != nil {
		return
	}
	err = en.WriteTime(z.lastHandout)
	if err != nil {
		err = msgp.WrapError(err, "lastHandout")
		return
	}
	// write "stc"
	err = en.Append(0xa3, 0x73, 0x74, 0x63)
	if err != nil {
		return
	}
	err = en.WriteUint64(z.startedCycle)
	if err != nil {
		err = msgp.WrapError(err, "startedCycle")
		return
	}
	// write "endc"
	err = en.Append(0xa4, 0x65, 0x6e, 0x64, 0x63)
	if err != nil {
		return
	}
	err = en.WriteUint64(z.endedCycle)
	if err != nil {
		err = msgp.WrapError(err, "endedCycle")
		return
	}
	// write "v"
	err = en.Append(0xa1, 0x76)
	if err != nil {
		return
	}
	err = en.WriteUint8(z.dataVersion)
	if err != nil {
		err = msgp.WrapError(err, "dataVersion")
		return
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z *metacache) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// map header, size 14
	// string "id"
	o = append(o, 0x8e, 0xa2, 0x69, 0x64)
	o = msgp.AppendString(o, z.id)
	// string "b"
	o = append(o, 0xa1, 0x62)
	o = msgp.AppendString(o, z.bucket)
	// string "root"
	o = append(o, 0xa4, 0x72, 0x6f, 0x6f, 0x74)
	o = msgp.AppendString(o, z.root)
	// string "rec"
	o = append(o, 0xa3, 0x72, 0x65, 0x63)
	o = msgp.AppendBool(o, z.recursive)
	// string "stat"
	o = append(o, 0xa4, 0x73, 0x74, 0x61, 0x74)
	o = msgp.AppendUint8(o, uint8(z.status))
	// string "fnf"
	o = append(o, 0xa3, 0x66, 0x6e, 0x66)
	o = msgp.AppendBool(o, z.fileNotFound)
	// string "err"
	o = append(o, 0xa3, 0x65, 0x72, 0x72)
	o = msgp.AppendString(o, z.error)
	// string "st"
	o = append(o, 0xa2, 0x73, 0x74)
	o = msgp.AppendTime(o, z.started)
	// string "end"
	o = append(o, 0xa3, 0x65, 0x6e, 0x64)
	o = msgp.AppendTime(o, z.ended)
	// string "u"
	o = append(o, 0xa1, 0x75)
	o = msgp.AppendTime(o, z.lastUpdate)
	// string "lh"
	o = append(o, 0xa2, 0x6c, 0x68)
	o = msgp.AppendTime(o, z.lastHandout)
	// string "stc"
	o = append(o, 0xa3, 0x73, 0x74, 0x63)
	o = msgp.AppendUint64(o, z.startedCycle)
	// string "endc"
	o = append(o, 0xa4, 0x65, 0x6e, 0x64, 0x63)
	o = msgp.AppendUint64(o, z.endedCycle)
	// string "v"
	o = append(o, 0xa1, 0x76)
	o = msgp.AppendUint8(o, z.dataVersion)
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *metacache) UnmarshalMsg(bts []byte) (o []byte, err error) {
	var field []byte
	_ = field
	var zb0001 uint32
	zb0001, bts, err = msgp.ReadMapHeaderBytes(bts)
	if err != nil {
		err = msgp.WrapError(err)
		return
	}
	for zb0001 > 0 {
		zb0001--
		field, bts, err = msgp.ReadMapKeyZC(bts)
		if err != nil {
			err = msgp.WrapError(err)
			return
		}
		switch msgp.UnsafeString(field) {
		case "id":
			z.id, bts, err = msgp.ReadStringBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "id")
				return
			}
		case "b":
			z.bucket, bts, err = msgp.ReadStringBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "bucket")
				return
			}
		case "root":
			z.root, bts, err = msgp.ReadStringBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "root")
				return
			}
		case "rec":
			z.recursive, bts, err = msgp.ReadBoolBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "recursive")
				return
			}
		case "stat":
			{
				var zb0002 uint8
				zb0002, bts, err = msgp.ReadUint8Bytes(bts)
				if err != nil {
					err = msgp.WrapError(err, "status")
					return
				}
				z.status = scanStatus(zb0002)
			}
		case "fnf":
			z.fileNotFound, bts, err = msgp.ReadBoolBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "fileNotFound")
				return
			}
		case "err":
			z.error, bts, err = msgp.ReadStringBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "error")
				return
			}
		case "st":
			z.started, bts, err = msgp.ReadTimeBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "started")
				return
			}
		case "end":
			z.ended, bts, err = msgp.ReadTimeBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "ended")
				return
			}
		case "u":
			z.lastUpdate, bts, err = msgp.ReadTimeBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "lastUpdate")
				return
			}
		case "lh":
			z.lastHandout, bts, err = msgp.ReadTimeBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "lastHandout")
				return
			}
		case "stc":
			z.startedCycle, bts, err = msgp.ReadUint64Bytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "startedCycle")
				return
			}
		case "endc":
			z.endedCycle, bts, err = msgp.ReadUint64Bytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "endedCycle")
				return
			}
		case "v":
			z.dataVersion, bts, err = msgp.ReadUint8Bytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "dataVersion")
				return
			}
		default:
			bts, err = msgp.Skip(bts)
			if err != nil {
				err = msgp.WrapError(err)
				return
			}
		}
	}
	o = bts
	return
}

// Msgsize returns an upper bound estimate of the number of bytes occupied by the serialized message
func (z *metacache) Msgsize() (s int) {
	s = 1 + 3 + msgp.StringPrefixSize + len(z.id) + 2 + msgp.StringPrefixSize + len(z.bucket) + 5 + msgp.StringPrefixSize + len(z.root) + 4 + msgp.BoolSize + 5 + msgp.Uint8Size + 4 + msgp.BoolSize + 4 + msgp.StringPrefixSize + len(z.error) + 3 + msgp.TimeSize + 4 + msgp.TimeSize + 2 + msgp.TimeSize + 3 + msgp.TimeSize + 4 + msgp.Uint64Size + 5 + msgp.Uint64Size + 2 + msgp.Uint8Size
	return
}

// DecodeMsg implements msgp.Decodable
func (z *scanStatus) DecodeMsg(dc *msgp.Reader) (err error) {
	{
		var zb0001 uint8
		zb0001, err = dc.ReadUint8()
		if err != nil {
			err = msgp.WrapError(err)
			return
		}
		(*z) = scanStatus(zb0001)
	}
	return
}

// EncodeMsg implements msgp.Encodable
func (z scanStatus) EncodeMsg(en *msgp.Writer) (err error) {
	err = en.WriteUint8(uint8(z))
	if err != nil {
		err = msgp.WrapError(err)
		return
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z scanStatus) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	o = msgp.AppendUint8(o, uint8(z))
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *scanStatus) UnmarshalMsg(bts []byte) (o []byte, err error) {
	{
		var zb0001 uint8
		zb0001, bts, err = msgp.ReadUint8Bytes(bts)
		if err != nil {
			err = msgp.WrapError(err)
			return
		}
		(*z) = scanStatus(zb0001)
	}
	o = bts
	return
}

// Msgsize returns an upper bound estimate of the number of bytes occupied by the serialized message
func (z scanStatus) Msgsize() (s int) {
	s = msgp.Uint8Size
	return
}