package check

import "fmt"

// KVOp is the kind of a key-value operation.
type KVOp uint8

const (
	KVGet KVOp = iota + 1
	KVPut
	KVDelete
)

// KVInput is a key-value operation as the client issued it.
type KVInput struct {
	Op    KVOp
	Key   string
	Value string // for KVPut
}

// KVOutput is what the client observed. Found and Value matter for KVGet
// only.
type KVOutput struct {
	Value string
	Found bool
}

// KVState is the model state of one key.
type KVState struct {
	Value string
	Found bool
}

// KVModel is a linearizable map from string keys to string values, with
// get, put and delete. It partitions by key, so each key is checked on its
// own.
//
// Give every put a unique value. A checker can only catch a stale read if
// it can tell the two values apart.
//
// Verified:
//   - [kv-semantics] A get returns the latest put's value on its key, or not-
//     found if none or deleted since. Puts and deletes accept any output.
//   - [kv-unknown-gets-are-free] A get with an unknown outcome changes
//     nothing and is always legal.
func KVModel() Model[KVState, KVInput, KVOutput] {
	return Model[KVState, KVInput, KVOutput]{
		Init: func() KVState { return KVState{} },
		Step: func(s KVState, in KVInput, out KVOutput, known bool) (bool, KVState) {
			switch in.Op {
			case KVPut:
				return true, KVState{Value: in.Value, Found: true}
			case KVDelete:
				return true, KVState{}
			case KVGet:
				if !known {
					return true, s
				}
				return out.Found == s.Found && (!s.Found || out.Value == s.Value), s
			}
			return false, s
		},
		Partition: func(in KVInput) string { return in.Key },
		Describe: func(in KVInput, out KVOutput, known bool) string {
			switch in.Op {
			case KVPut:
				return fmt.Sprintf("put(%q, %q)%s", in.Key, in.Value, unknownMark(known))
			case KVDelete:
				return fmt.Sprintf("delete(%q)%s", in.Key, unknownMark(known))
			default:
				switch {
				case !known:
					return fmt.Sprintf("get(%q) -> ?", in.Key)
				case !out.Found:
					return fmt.Sprintf("get(%q) -> <none>", in.Key)
				default:
					return fmt.Sprintf("get(%q) -> %q", in.Key, out.Value)
				}
			}
		},
	}
}

func unknownMark(known bool) string {
	if known {
		return ""
	}
	return " ?"
}
