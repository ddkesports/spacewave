package kvtx_block_okra

import (
	"bytes"

	protobuf_go_lite "github.com/aperturerobotics/protobuf-go-lite"
	"github.com/s4wave/spacewave/db/block"
	"github.com/s4wave/spacewave/db/block/blob"
	"github.com/s4wave/spacewave/net/hash"
)

// Wire tags of the page, entry, block ref and hash fields. Varint fields have
// wire type 0, so their tag is the field number shifted.
const (
	pageTagLevel          = 1 << 3
	pageTagLowerBound     = 2<<3 | 2
	pageTagUpperBound     = 3<<3 | 2
	pageTagStartsAtAnchor = 4 << 3
	pageTagSize           = 5 << 3
	pageTagPageHash       = 6<<3 | 2
	pageTagEntries        = 7<<3 | 2

	entryTagAnchor      = 1 << 3
	entryTagKey         = 2<<3 | 2
	entryTagHash        = 3<<3 | 2
	entryTagSize        = 4 << 3
	entryTagChildRef    = 5<<3 | 2
	entryTagValueRef    = 6<<3 | 2
	entryTagValueIsBlob = 7 << 3
	entryTagValueBlob   = 8<<3 | 2

	blockRefTagHash = 1<<3 | 2

	hashTagHashType = 1 << 3
	hashTagHash     = 2<<3 | 2
)

// UnmarshalDecodedBlock decodes a page read through a block cursor. It keeps
// one copy of data and carves every entry, block ref and hash from slices
// sized for the page, so a page costs a fixed handful of allocations instead of
// about six per entry. The page is the one UnmarshalVT produces: input outside
// the canonical encoding (a repeated or unknown field) and every error go to
// UnmarshalVT, as does a page that is not empty.
func (p *Page) UnmarshalDecodedBlock(data []byte) error {
	// Merge into a page that is not empty as UnmarshalVT does.
	if !p.isEmpty() {
		return p.UnmarshalVT(data)
	}

	// Decode into slabs, leaving input they do not accept to UnmarshalVT.
	var next Page
	if !decodePageSlab(&next, data) {
		return p.UnmarshalVT(data)
	}
	*p = next
	return nil
}

// isEmpty reports whether p holds no decoded field.
func (p *Page) isEmpty() bool {
	return p.Level == 0 && p.LowerBound == nil && p.UpperBound == nil &&
		!p.StartsAtAnchor && p.Size == 0 && p.PageHash == nil &&
		p.Entries == nil && p.unknownFields == nil
}

// pageSlab holds the slices a page decode carves its messages from.
type pageSlab struct {
	// buf is the page's copy of the encoded bytes. Bytes fields point into it.
	buf []byte
	// refs and hashes hold the block refs not yet handed out.
	refs   []block.BlockRef
	hashes []hash.Hash
}

// decodePageSlab decodes data into page, which must be empty. It returns
// false for input it does not accept, leaving page partly set.
func decodePageSlab(page *Page, data []byte) bool {
	// Count the entries and refs, requiring every other field at most once.
	n, nrefs, ok := countPageEntries(data)
	if !ok {
		return false
	}

	// Copy the input once and allocate the entries and refs.
	d := &pageSlab{buf: bytes.Clone(data)}
	var ents []Entry
	if n != 0 {
		ents = make([]Entry, n)
		page.Entries = make([]*Entry, 0, n)
	}
	if nrefs != 0 {
		d.refs = make([]block.BlockRef, nrefs)
		d.hashes = make([]hash.Hash, nrefs)
	}

	// Decode the fields, pointing bytes fields into the copy.
	b := d.buf
	for i := 0; i < len(b); {
		wire, ni, err := protobuf_go_lite.DecodeVarint(b, i)
		if err != nil {
			return false
		}
		switch wire {
		case pageTagLevel:
			page.Level, i, err = protobuf_go_lite.DecodeVarintUint32(b, ni)
		case pageTagLowerBound:
			page.LowerBound, i, err = d.bytes(b, ni)
		case pageTagUpperBound:
			page.UpperBound, i, err = d.bytes(b, ni)
		case pageTagStartsAtAnchor:
			page.StartsAtAnchor, i, err = protobuf_go_lite.DecodeVarintBool(b, ni)
		case pageTagSize:
			page.Size, i, err = protobuf_go_lite.DecodeVarint(b, ni)
		case pageTagPageHash:
			page.PageHash, i, err = d.bytes(b, ni)
		case pageTagEntries:
			var start, end int
			start, end, err = protobuf_go_lite.DecodeLengthDelimited(b, ni)
			if err != nil {
				return false
			}
			ent := &ents[len(page.Entries)]
			if !d.entry(ent, start, end) {
				return false
			}
			page.Entries = append(page.Entries, ent)
			i = end
		default:
			return false
		}
		if err != nil {
			return false
		}
	}
	return len(page.Entries) == n
}

// countPageEntries counts the entries of an encoded page and the block refs
// they carry. It returns false for an unknown page field, a repeated non-entry
// field, or a malformed tag or length.
func countPageEntries(b []byte) (int, int, bool) {
	var n, nrefs int
	var seen uint8
	for i := 0; i < len(b); {
		wire, ni, err := protobuf_go_lite.DecodeVarint(b, i)
		if err != nil {
			return 0, 0, false
		}
		switch wire {
		case pageTagEntries:
			n++
		case pageTagLevel, pageTagLowerBound, pageTagUpperBound,
			pageTagStartsAtAnchor, pageTagSize, pageTagPageHash:
			bit := uint8(1) << (wire >> 3)
			if seen&bit != 0 {
				return 0, 0, false
			}
			seen |= bit
		default:
			return 0, 0, false
		}
		if wire&7 != 2 {
			_, i, err = protobuf_go_lite.DecodeVarint(b, ni)
			if err != nil {
				return 0, 0, false
			}
			continue
		}
		start, end, err := protobuf_go_lite.DecodeLengthDelimited(b, ni)
		if err != nil {
			return 0, 0, false
		}
		if wire == pageTagEntries {
			refs, ok := countEntryRefs(b[:end], start)
			if !ok {
				return 0, 0, false
			}
			nrefs += refs
		}
		i = end
	}
	return n, nrefs, true
}

// countEntryRefs counts the block refs of the entry encoded at b[start:].
func countEntryRefs(b []byte, start int) (int, bool) {
	var n int
	for i := start; i < len(b); {
		wire, ni, err := protobuf_go_lite.DecodeVarint(b, i)
		if err != nil {
			return 0, false
		}
		switch wire & 7 {
		case 0:
			_, i, err = protobuf_go_lite.DecodeVarint(b, ni)
		case 2:
			if wire == entryTagChildRef || wire == entryTagValueRef {
				n++
			}
			_, i, err = protobuf_go_lite.DecodeLengthDelimited(b, ni)
		default:
			return 0, false
		}
		if err != nil {
			return 0, false
		}
	}
	return n, true
}

// entry decodes the entry encoded at buf[start:end] into ent.
func (d *pageSlab) entry(ent *Entry, start, end int) bool {
	b := d.buf[:end]
	var seen uint16
	for i := start; i < end; {
		wire, ni, err := protobuf_go_lite.DecodeVarint(b, i)
		if err != nil {
			return false
		}
		field := wire >> 3
		if field == 0 || field > 8 || seen&(1<<field) != 0 {
			return false
		}
		seen |= 1 << field
		switch wire {
		case entryTagAnchor:
			ent.Anchor, i, err = protobuf_go_lite.DecodeVarintBool(b, ni)
		case entryTagKey:
			ent.Key, i, err = d.bytes(b, ni)
		case entryTagHash:
			ent.Hash, i, err = d.bytes(b, ni)
		case entryTagSize:
			ent.Size, i, err = protobuf_go_lite.DecodeVarint(b, ni)
		case entryTagChildRef, entryTagValueRef:
			var ref *block.BlockRef
			var ok bool
			ref, i, ok = d.blockRef(b, ni)
			if !ok {
				return false
			}
			if wire == entryTagChildRef {
				ent.ChildRef = ref
			} else {
				ent.ValueRef = ref
			}
		case entryTagValueIsBlob:
			ent.ValueIsBlob, i, err = protobuf_go_lite.DecodeVarintBool(b, ni)
		case entryTagValueBlob:
			var vs, ve int
			vs, ve, err = protobuf_go_lite.DecodeLengthDelimited(b, ni)
			if err != nil {
				return false
			}
			ent.ValueBlob = &blob.Blob{}
			err = ent.ValueBlob.UnmarshalVT(b[vs:ve])
			i = ve
		default:
			return false
		}
		if err != nil {
			return false
		}
	}
	return true
}

// blockRef decodes the length-delimited block ref at b[idx:].
func (d *pageSlab) blockRef(b []byte, idx int) (*block.BlockRef, int, bool) {
	// Find the encoded ref.
	start, end, err := protobuf_go_lite.DecodeLengthDelimited(b, idx)
	if err != nil {
		return nil, 0, false
	}

	// Take a ref and hash from the slabs, sized by the counting pass.
	if len(d.refs) == 0 {
		return nil, 0, false
	}
	ref, h := &d.refs[0], &d.hashes[0]
	d.refs, d.hashes = d.refs[1:], d.hashes[1:]

	// Decode the ref's only field, its hash.
	b = b[:end]
	for i := start; i < end; {
		wire, ni, err := protobuf_go_lite.DecodeVarint(b, i)
		if err != nil || wire != blockRefTagHash || ref.Hash != nil {
			return nil, 0, false
		}
		hs, he, err := protobuf_go_lite.DecodeLengthDelimited(b, ni)
		if err != nil || !d.hash(h, b[:he], hs) {
			return nil, 0, false
		}
		ref.Hash = h
		i = he
	}
	return ref, end, true
}

// hash decodes the hash encoded at b[start:] into h.
func (d *pageSlab) hash(h *hash.Hash, b []byte, start int) bool {
	var seen uint8
	for i := start; i < len(b); {
		wire, ni, err := protobuf_go_lite.DecodeVarint(b, i)
		if err != nil {
			return false
		}
		switch wire {
		case hashTagHashType:
			if seen&1 != 0 {
				return false
			}
			seen |= 1
			var v uint64
			v, i, err = protobuf_go_lite.DecodeVarint(b, ni)
			h.HashType = hash.HashType(v) //nolint:gosec // matches the generated decoder's conversion.
		case hashTagHash:
			if seen&2 != 0 {
				return false
			}
			seen |= 2
			h.Hash, i, err = d.bytes(b, ni)
		default:
			return false
		}
		if err != nil {
			return false
		}
	}
	return true
}

// bytes returns the length-delimited bytes at b[idx:] from the page's copy,
// capped so an append cannot write into the next field.
func (d *pageSlab) bytes(b []byte, idx int) ([]byte, int, error) {
	start, end, err := protobuf_go_lite.DecodeLengthDelimited(b, idx)
	if err != nil {
		return nil, 0, err
	}
	return d.buf[start:end:end], end, nil
}

// _ is a type assertion
var _ block.DecodedBlockUnmarshaler = (*Page)(nil)
