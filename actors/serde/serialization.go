package serde

import (
	"bytes"

	"github.com/filecoin-project/specs-actors/actors/runtime"
	autil "github.com/filecoin-project/specs-actors/actors/util"
)

// Serializes a structure or value to CBOR.
func Serialize(o interface{}) ([]byte, error) {
	cm, ok := o.(runtime.CBORMarshaler)
	if ok {
		buf := new(bytes.Buffer)
		err := cm.MarshalCBOR(buf)
		autil.AssertNoError(err)
		return buf.Bytes(), nil
	}
	autil.TODO("CBOR-serialization")
	return nil, nil
}

func MustSerialize(o interface{}) []byte {
	s, err := Serialize(o)
	autil.AssertMsg(err == nil, "serialization failed")
	return s
}

// Serializes an array of method invocation params.
func MustSerializeParams(o ...interface{}) []byte {
	return MustSerialize(o)
}

// Deserializes a structure or value from CBOR.
func Deserialize(b []byte, out interface{}) error {
	autil.TODO("CBOR-deserialization")
	return nil
}

func MustDeserialize(b []byte, out interface{}) {
	err := Deserialize(b, out)
	autil.AssertMsg(err == nil, "deserialization failed")
}
