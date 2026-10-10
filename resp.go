package rueidis

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"sync"
	"unsafe"
)

var errChunked = errors.New("unbounded redis message")
var errOldNull = errors.New("RESP2 null")

const (
	typeBlobString     = byte('$')
	typeSimpleString   = byte('+')
	typeSimpleErr      = byte('-')
	typeInteger        = byte(':')
	typeNull           = byte('_')
	typeEnd            = byte('.')
	typeFloat          = byte(',')
	typeBool           = byte('#')
	typeBlobErr        = byte('!')
	typeVerbatimString = byte('=')
	typeBigNumber      = byte('(')
	typeArray          = byte('*')
	typeMap            = byte('%')
	typeSet            = byte('~')
	typeAttribute      = byte('|')
	typePush           = byte('>')
	typeChunk          = byte(';')
)

var typeNames = make(map[byte]string, 16)

type reader func(i *bufio.Reader) (RedisMessage, error)

var readers = [256]reader{}

func init() {
	readers[typeBlobString] = readBlobString
	readers[typeSimpleString] = readSimpleString
	readers[typeSimpleErr] = readSimpleString
	readers[typeInteger] = readInteger
	readers[typeNull] = readNull
	readers[typeFloat] = readSimpleString
	readers[typeBool] = readBoolean
	readers[typeBlobErr] = readBlobString
	readers[typeVerbatimString] = readBlobString
	readers[typeBigNumber] = readSimpleString
	readers[typeArray] = readArray
	readers[typeMap] = readMap
	readers[typeSet] = readArray
	readers[typeAttribute] = readMap
	readers[typePush] = readArray
	readers[typeEnd] = readNull

	typeNames[typeBlobString] = "blob string"
	typeNames[typeSimpleString] = "simple string"
	typeNames[typeSimpleErr] = "simple error"
	typeNames[typeInteger] = "int64"
	typeNames[typeNull] = "null"
	typeNames[typeFloat] = "float64"
	typeNames[typeBool] = "boolean"
	typeNames[typeBlobErr] = "blob error"
	typeNames[typeVerbatimString] = "verbatim string"
	typeNames[typeBigNumber] = "big number"
	typeNames[typeArray] = "array"
	typeNames[typeMap] = "map"
	typeNames[typeSet] = "set"
	typeNames[typeAttribute] = "attribute"
	typeNames[typePush] = "push"
	typeNames[typeEnd] = "null"
}

func readSimpleString(i *bufio.Reader) (m RedisMessage, err error) {
	m.bytes, m.intlen, err = readS(i)
	return
}

func readBlobString(i *bufio.Reader) (m RedisMessage, err error) {
	m.bytes, m.intlen, err = readB(i)
	if err == errChunked {
		sb := strings.Builder{}
		for {
			if _, err = i.Discard(1); err != nil { // discard the ';'
				return RedisMessage{}, err
			}
			length, err := readI(i)
			if err != nil {
				return RedisMessage{}, err
			}
			if length == 0 {
				m.setString(sb.String())
				return m, nil
			}
			sb.Grow(int(length))
			if _, err = io.CopyN(&sb, i, length); err != nil {
				return RedisMessage{}, err
			}
			if _, err = i.Discard(2); err != nil {
				return RedisMessage{}, err
			}
		}
	}
	return
}

func readInteger(i *bufio.Reader) (m RedisMessage, err error) {
	m.intlen, err = readI(i)
	return
}

func readBoolean(i *bufio.Reader) (m RedisMessage, err error) {
	b, err := i.ReadByte()
	if err != nil {
		return RedisMessage{}, err
	}
	if b == 't' {
		m.intlen = 1
	}
	_, err = i.Discard(2)
	return
}

func readNull(i *bufio.Reader) (m RedisMessage, err error) {
	_, err = i.Discard(2)
	return
}

func readArray(i *bufio.Reader) (m RedisMessage, err error) {
	length, err := readI(i)
	if err == nil {
		if length == -1 {
			return m, errOldNull
		}
		m.array, m.intlen, err = readA(i, length)
	} else if err == errChunked {
		m.array, m.intlen, err = readE(i)
	}
	return m, err
}

func readMap(i *bufio.Reader) (m RedisMessage, err error) {
	length, err := readI(i)
	if err == nil {
		m.array, m.intlen, err = readA(i, length*2)
	} else if err == errChunked {
		m.array, m.intlen, err = readE(i)
	}
	return m, err
}

const ok = "OK"
const okrn = "OK\r\n"

func readS(i *bufio.Reader) (*byte, int64, error) {
	if peek, _ := i.Peek(2); string(peek) == ok {
		if peek, _ = i.Peek(4); string(peek) == okrn {
			_, _ = i.Discard(4)
			return unsafe.StringData(ok), int64(len(ok)), nil
		}
	}
	bs, err := i.ReadBytes('\n')
	if err != nil {
		return nil, 0, err
	}
	if trim := len(bs) - 2; trim < 0 {
		return nil, 0, errors.New(unexpectedNoCRLF)
	} else {
		bs = bs[:trim]
	}
	return unsafe.SliceData(bs), int64(len(bs)), nil
}

func readI(i *bufio.Reader) (v int64, err error) {
	bs, err := i.ReadSlice('\n')
	if err != nil {
		return 0, err
	}
	if len(bs) < 3 {
		return 0, errors.New(unexpectedNoCRLF)
	}
	return parseIntLine(bs)
}

func parseIntLine(bs []byte) (v int64, err error) {
	if bs[0] == '?' {
		return 0, errChunked
	}
	var s = int64(1)
	if bs[0] == '-' {
		s = -1
		bs = bs[1:]
	}
	for _, c := range bs[:len(bs)-2] {
		if d := int64(c - '0'); d >= 0 && d <= 9 {
			v = v*10 + d
		} else {
			return 0, errors.New(unexpectedNumByte + strconv.Itoa(int(c)))
		}
	}
	return v * s, nil
}

func readB(i *bufio.Reader) (*byte, int64, error) {
	length, err := readI(i)
	if err != nil {
		return nil, 0, err
	}
	if length == -1 {
		return nil, 0, errOldNull
	}
	bs := make([]byte, length)
	if _, err = io.ReadFull(i, bs); err != nil {
		return nil, 0, err
	}
	if _, err = i.Discard(2); err != nil {
		return nil, 0, err
	}
	return unsafe.SliceData(bs), int64(len(bs)), nil
}

func readE(i *bufio.Reader) (*RedisMessage, int64, error) {
	v := make([]RedisMessage, 0)
	for {
		n, err := readNextMessage(i)
		if err != nil {
			return nil, 0, err
		}
		if n.typ == '.' {
			return unsafe.SliceData(v), int64(len(v)), err
		}
		v = append(v, n)
	}
}

func readA(i *bufio.Reader, length int64) (*RedisMessage, int64, error) {
	var err error

	msgs := make([]RedisMessage, length)
	for n := range length {
		if msgs[n], err = readNextMessage(i); err != nil {
			return nil, 0, err
		}
	}
	return unsafe.SliceData(msgs), length, nil
}

func writeB(o *bufio.Writer, id byte, str string) (err error) {
	_ = writeN(o, id, len(str))
	_, _ = o.WriteString(str)
	_, err = o.WriteString("\r\n")
	return err
}

func writeS(o *bufio.Writer, id byte, str string) (err error) {
	_ = o.WriteByte(id)
	_, _ = o.WriteString(str)
	_, err = o.WriteString("\r\n")
	return err
}

func writeN(o *bufio.Writer, id byte, n int) (err error) {
	_ = o.WriteByte(id)
	if n < 10 {
		_ = o.WriteByte(byte('0' + n))
	} else {
		for d := int(math.Pow10(int(math.Log10(float64(n))))); d > 0; d /= 10 {
			_ = o.WriteByte(byte('0' + n/d))
			n = n % d
		}
	}
	_, err = o.WriteString("\r\n")
	return err
}

func readNextMessage(i *bufio.Reader) (m RedisMessage, err error) {
	var attrs *RedisMessage
	var typ byte
	for {
		if typ, err = i.ReadByte(); err != nil {
			return RedisMessage{}, err
		}
		fn := readers[typ]
		if fn == nil {
			return RedisMessage{}, errors.New(unknownMessageType + strconv.Itoa(int(typ)))
		}
		if m, err = fn(i); err != nil {
			if err == errOldNull {
				return RedisMessage{typ: typeNull}, nil
			}
			return RedisMessage{}, err
		}
		m.typ = typ
		if m.typ == typeAttribute { // handle the attributes
			a := m     // clone the original m first, and then take the address of the clone
			attrs = &a // to avoid go compiler allocating the m on heap which causing worse performance.
			m = RedisMessage{}
			continue
		}
		m.attrs = attrs
		return m, nil
	}
}

// readLimit configures the per-message safety limits. A zero value disables all limits.
type readLimit struct {
	maxBytes int64 // maximum accumulated wire bytes of a single message
	maxElems int64 // maximum accumulated element count of a single message
	maxDepth int   // maximum nesting depth of a single message
}

func (l readLimit) enabled() bool {
	return l.maxBytes > 0 || l.maxElems > 0 || l.maxDepth > 0
}

// limitedReader reads a RESP message while enforcing byte, element and nesting-depth limits.
// Once any limit is exceeded, limitedReader enters a sticky discarding mode: it keeps walking
// and draining the rest of the current message so the connection stays usable for the following
// replies, but it stops allocating payloads for the message that exceeded the limit.
type limitedReader struct {
	i        *bufio.Reader
	l        readLimit
	bytes    int64
	elems    int64
	depth    int
	exceeded error
}

func (lr *limitedReader) reset(i *bufio.Reader, l readLimit) {
	lr.i = i
	lr.l = l
	lr.bytes = 0
	lr.elems = 0
	lr.depth = 0
	lr.exceeded = nil
}

func (lr *limitedReader) addBytes(n int64) {
	if lr.exceeded != nil {
		return
	}
	lr.bytes += n
	if lr.l.maxBytes > 0 && lr.bytes > lr.l.maxBytes {
		lr.exceeded = ErrMessageSizeExceeded
	}
}

func (lr *limitedReader) addElements(n int64) {
	if lr.exceeded != nil {
		return
	}
	lr.elems += n
	if lr.l.maxElems > 0 && lr.elems > lr.l.maxElems {
		lr.exceeded = ErrMessageElementsExceeded
	}
}

// reserveElements checks whether n more elements can fit without adding them.
// Aggregate children count themselves when they are read, but the reservation
// is done before allocating the aggregate backing slice so that a huge declared
// length cannot exhaust the memory.
func (lr *limitedReader) reserveElements(n int64) {
	if lr.exceeded != nil {
		return
	}
	if lr.l.maxElems > 0 && lr.elems+n > lr.l.maxElems {
		lr.exceeded = ErrMessageElementsExceeded
	}
}

func (lr *limitedReader) enterDepth() {
	if lr.exceeded != nil {
		return
	}
	lr.depth++
	if lr.l.maxDepth > 0 && lr.depth > lr.l.maxDepth {
		lr.exceeded = ErrMessageDepthExceeded
	}
}

func (lr *limitedReader) leaveDepth() { lr.depth-- }

func (lr *limitedReader) discard(n int64) error {
	_, err := lr.i.Discard(int(n))
	return err
}

// readNext reads one complete message tree. The returned message keeps its type
// even when a limit is exceeded, which lets callers distinguish pushes from command replies.
func (lr *limitedReader) readNext() (m RedisMessage, err error) {
	var attrs *RedisMessage
	for {
		var typ byte
		if typ, err = lr.i.ReadByte(); err != nil {
			return RedisMessage{}, err
		}
		if typ != typeEnd { // the chunked end marker is not a delivered element
			lr.addElements(1)
		}

		switch typ {
		case typeBlobString, typeBlobErr, typeVerbatimString:
			m, err = lr.readBlob()
		case typeSimpleString, typeSimpleErr, typeFloat, typeBigNumber:
			m, err = lr.readSimple()
		case typeInteger:
			m, err = lr.readInteger()
		case typeNull, typeEnd:
			lr.addBytes(2)
			err = lr.discard(2)
		case typeBool:
			m, err = lr.readBool()
		case typeArray, typeSet, typePush:
			m, err = lr.readArray()
		case typeMap, typeAttribute:
			m, err = lr.readMap()
		default:
			return RedisMessage{}, errors.New(unknownMessageType + strconv.Itoa(int(typ)))
		}
		if err != nil {
			if err == errOldNull {
				return RedisMessage{typ: typeNull}, lr.exceeded
			}
			return RedisMessage{typ: typ}, err
		}
		m.typ = typ
		if typ == typeAttribute { // handle the attributes, even in discarding mode
			a := m
			attrs = &a
			m = RedisMessage{}
			continue
		}
		if lr.exceeded != nil {
			return RedisMessage{typ: typ}, lr.exceeded
		}
		m.attrs = attrs
		return m, nil
	}
}

func (lr *limitedReader) readSimple() (m RedisMessage, err error) {
	bs, err := lr.i.ReadBytes('\n')
	if err != nil {
		return RedisMessage{}, err
	}
	lr.addBytes(int64(len(bs)))
	if len(bs) < 2 {
		return RedisMessage{}, errors.New(unexpectedNoCRLF)
	}
	if lr.exceeded != nil {
		return RedisMessage{}, lr.exceeded
	}
	trim := len(bs) - 2
	m.bytes = unsafe.SliceData(bs[:trim])
	m.intlen = int64(trim)
	return m, nil
}

func (lr *limitedReader) readInteger() (m RedisMessage, err error) {
	m.intlen, err = lr.readI()
	return m, err
}

func (lr *limitedReader) readI() (v int64, err error) {
	bs, err := lr.i.ReadSlice('\n')
	if err != nil {
		return 0, err
	}
	lr.addBytes(int64(len(bs)))
	return parseIntLine(bs)
}

func (lr *limitedReader) readBool() (m RedisMessage, err error) {
	b, err := lr.i.ReadByte()
	if err != nil {
		return RedisMessage{}, err
	}
	lr.addBytes(4)
	if _, err = lr.i.Discard(2); err != nil {
		return RedisMessage{}, err
	}
	if b == 't' {
		m.intlen = 1
	}
	if lr.exceeded != nil {
		return RedisMessage{}, lr.exceeded
	}
	return m, nil
}

func (lr *limitedReader) readBlob() (m RedisMessage, err error) {
	length, err := lr.readI()
	if err != nil {
		if err == errChunked {
			return lr.readChunkedBlob()
		}
		return RedisMessage{}, err
	}
	if length == -1 {
		return m, errOldNull
	}
	lr.addBytes(length + 2)
	if lr.exceeded != nil {
		if err = lr.discard(length + 2); err != nil {
			return RedisMessage{}, err
		}
		return RedisMessage{}, lr.exceeded
	}
	bs := make([]byte, length)
	if _, err = io.ReadFull(lr.i, bs); err != nil {
		return RedisMessage{}, err
	}
	if _, err = lr.i.Discard(2); err != nil {
		return RedisMessage{}, err
	}
	m.bytes = unsafe.SliceData(bs)
	m.intlen = length
	return m, nil
}

func (lr *limitedReader) readChunkedBlob() (m RedisMessage, err error) {
	var sb strings.Builder
	for {
		if _, err = lr.i.Discard(1); err != nil { // discard the ';'
			return RedisMessage{}, err
		}
		var length int64
		if length, err = lr.readI(); err != nil {
			return RedisMessage{}, err
		}
		if length == 0 {
			if lr.exceeded != nil {
				return RedisMessage{}, lr.exceeded
			}
			m.setString(sb.String())
			return m, nil
		}
		lr.addBytes(length + 2)
		if lr.exceeded != nil {
			if err = lr.discard(length + 2); err != nil {
				return RedisMessage{}, err
			}
			continue
		}
		sb.Grow(int(length))
		if _, err = io.CopyN(&sb, lr.i, length); err != nil {
			return RedisMessage{}, err
		}
		if _, err = lr.i.Discard(2); err != nil {
			return RedisMessage{}, err
		}
	}
}

func (lr *limitedReader) readArray() (m RedisMessage, err error) {
	length, err := lr.readI()
	if err != nil {
		if err == errChunked {
			return lr.readChunkedElems()
		}
		return RedisMessage{}, err
	}
	if length == -1 {
		return m, errOldNull
	}
	return lr.readChildren(length)
}

func (lr *limitedReader) readMap() (m RedisMessage, err error) {
	length, err := lr.readI()
	if err != nil {
		if err == errChunked {
			return lr.readChunkedElems()
		}
		return RedisMessage{}, err
	}
	return lr.readChildren(length * 2)
}

func (lr *limitedReader) readChildren(length int64) (m RedisMessage, err error) {
	lr.enterDepth()
	defer lr.leaveDepth()

	lr.reserveElements(length)

	if lr.exceeded != nil {
		if err = lr.drainChildren(length); err != nil {
			return RedisMessage{}, err
		}
		return RedisMessage{}, lr.exceeded
	}

	msgs := make([]RedisMessage, length)
	for n := int64(0); n < length; n++ {
		if msgs[n], err = lr.readNext(); err != nil {
			if isMessageLimitErr(err) {
				if err = lr.drainChildren(length - n - 1); err != nil {
					return RedisMessage{}, err
				}
				return RedisMessage{}, lr.exceeded
			}
			return RedisMessage{}, err
		}
	}
	m.array = unsafe.SliceData(msgs)
	m.intlen = length
	return m, nil
}

func (lr *limitedReader) drainChildren(n int64) (err error) {
	for ; n > 0; n-- {
		if _, err = lr.readNext(); err != nil {
			if isMessageLimitErr(err) {
				err = nil
				continue
			}
			return err
		}
	}
	return nil
}

func (lr *limitedReader) readChunkedElems() (m RedisMessage, err error) {
	lr.enterDepth()
	defer lr.leaveDepth()

	v := make([]RedisMessage, 0)
	for {
		n, e := lr.readNext()
		if n.typ == typeEnd {
			if lr.exceeded != nil {
				return RedisMessage{}, lr.exceeded
			}
			m.array = unsafe.SliceData(v)
			m.intlen = int64(len(v))
			return m, nil
		}
		if e != nil {
			if !isMessageLimitErr(e) {
				return RedisMessage{}, e
			}
			continue
		}
		v = append(v, n)
	}
}

func isMessageLimitErr(err error) bool {
	return errors.Is(err, ErrMessageSizeExceeded) ||
		errors.Is(err, ErrMessageElementsExceeded) ||
		errors.Is(err, ErrMessageDepthExceeded)
}

var lrs = sync.Pool{New: func() any { return &io.LimitedReader{} }}

func streamTo(i *bufio.Reader, w io.Writer) (n int64, err error, clean bool) {
next:
	var typ byte
	if typ, err = i.ReadByte(); err != nil {
		return 0, err, false
	}
	switch typ {
	case typeBlobString, typeVerbatimString, typeChunk:
		if n, err = readI(i); err != nil {
			if err == errChunked {
				var nn int64
				nn, err, clean = streamTo(i, w)
				for n += nn; nn != 0 && clean && err == nil; n += nn {
					nn, err, clean = streamTo(i, w)
				}
			}
			return n, err, clean
		}
		if n == -1 {
			return 0, Nil, true
		}
		full := n + 2
		if n != 0 {
			lr := lrs.Get().(*io.LimitedReader)
			lr.R = i
			lr.N = n
			n, err = io.Copy(w, lr)
			lr.R = nil
			lrs.Put(lr)
		} else if typ == typeChunk {
			return n, err, true
		}
		if _, err2 := i.Discard(int(full - n)); err2 == nil {
			clean = true
		} else if err == nil {
			err = err2
		}
		return n, err, clean
	default:
		_ = i.UnreadByte()
		m, err := readNextMessage(i)
		if err != nil {
			return 0, err, false
		}
		switch m.typ {
		case typeSimpleString, typeFloat, typeBigNumber:
			n, err := w.Write([]byte(m.string()))
			return int64(n), err, true
		case typeNull:
			return 0, Nil, true
		case typeSimpleErr, typeBlobErr:
			mm := m
			return 0, (*RedisError)(&mm), true
		case typeInteger, typeBool:
			n, err := w.Write([]byte(strconv.FormatInt(m.intlen, 10)))
			return int64(n), err, true
		case typePush:
			goto next
		default:
			return 0, fmt.Errorf("unsupported redis %q response for streaming read", typeNames[typ]), true
		}
	}
}

func writeCmd(o *bufio.Writer, cmd []string) (err error) {
	err = writeN(o, '*', len(cmd))
	for _, m := range cmd {
		err = writeB(o, '$', m)
		// TODO: Can we set cmd[i] = "" here to allow GC to eagerly recycle memory?
		// Related: https://github.com/redis/rueidis/issues/364
	}
	return err
}

func flushCmd(o *bufio.Writer, cmd []string) (err error) {
	_ = writeCmd(o, cmd)
	return o.Flush()
}

const (
	unexpectedNoCRLF   = "received unexpected simple string message ending without CRLF"
	unexpectedNumByte  = "received unexpected number byte: "
	unknownMessageType = "received unknown message type: "
)
