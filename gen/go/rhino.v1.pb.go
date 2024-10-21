// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.34.2
// 	protoc        (unknown)
// source: rhino.v1.proto

package rhinov1

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type Error struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Message string `protobuf:"bytes,1,opt,name=message,proto3" json:"message,omitempty"`
	Code    string `protobuf:"bytes,2,opt,name=code,proto3" json:"code,omitempty"`
}

func (x *Error) Reset() {
	*x = Error{}
	if protoimpl.UnsafeEnabled {
		mi := &file_rhino_v1_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Error) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Error) ProtoMessage() {}

func (x *Error) ProtoReflect() protoreflect.Message {
	mi := &file_rhino_v1_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Error.ProtoReflect.Descriptor instead.
func (*Error) Descriptor() ([]byte, []int) {
	return file_rhino_v1_proto_rawDescGZIP(), []int{0}
}

func (x *Error) GetMessage() string {
	if x != nil {
		return x.Message
	}
	return ""
}

func (x *Error) GetCode() string {
	if x != nil {
		return x.Code
	}
	return ""
}

type SubscriptionRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Topic string `protobuf:"bytes,1,opt,name=topic,proto3" json:"topic,omitempty"`
}

func (x *SubscriptionRequest) Reset() {
	*x = SubscriptionRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_rhino_v1_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *SubscriptionRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*SubscriptionRequest) ProtoMessage() {}

func (x *SubscriptionRequest) ProtoReflect() protoreflect.Message {
	mi := &file_rhino_v1_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use SubscriptionRequest.ProtoReflect.Descriptor instead.
func (*SubscriptionRequest) Descriptor() ([]byte, []int) {
	return file_rhino_v1_proto_rawDescGZIP(), []int{1}
}

func (x *SubscriptionRequest) GetTopic() string {
	if x != nil {
		return x.Topic
	}
	return ""
}

type SubscriptionResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Id    string `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
	Topic string `protobuf:"bytes,2,opt,name=topic,proto3" json:"topic,omitempty"`
	Data  []byte `protobuf:"bytes,3,opt,name=data,proto3" json:"data,omitempty"`
	Error *Error `protobuf:"bytes,4,opt,name=error,proto3" json:"error,omitempty"`
}

func (x *SubscriptionResponse) Reset() {
	*x = SubscriptionResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_rhino_v1_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *SubscriptionResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*SubscriptionResponse) ProtoMessage() {}

func (x *SubscriptionResponse) ProtoReflect() protoreflect.Message {
	mi := &file_rhino_v1_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use SubscriptionResponse.ProtoReflect.Descriptor instead.
func (*SubscriptionResponse) Descriptor() ([]byte, []int) {
	return file_rhino_v1_proto_rawDescGZIP(), []int{2}
}

func (x *SubscriptionResponse) GetId() string {
	if x != nil {
		return x.Id
	}
	return ""
}

func (x *SubscriptionResponse) GetTopic() string {
	if x != nil {
		return x.Topic
	}
	return ""
}

func (x *SubscriptionResponse) GetData() []byte {
	if x != nil {
		return x.Data
	}
	return nil
}

func (x *SubscriptionResponse) GetError() *Error {
	if x != nil {
		return x.Error
	}
	return nil
}

type PublishRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Topic      string `protobuf:"bytes,1,opt,name=topic,proto3" json:"topic,omitempty"`
	Data       []byte `protobuf:"bytes,2,opt,name=data,proto3" json:"data,omitempty"`
	Ttl        int32  `protobuf:"varint,3,opt,name=ttl,proto3" json:"ttl,omitempty"`
	Persistent bool   `protobuf:"varint,4,opt,name=persistent,proto3" json:"persistent,omitempty"`
}

func (x *PublishRequest) Reset() {
	*x = PublishRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_rhino_v1_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *PublishRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*PublishRequest) ProtoMessage() {}

func (x *PublishRequest) ProtoReflect() protoreflect.Message {
	mi := &file_rhino_v1_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use PublishRequest.ProtoReflect.Descriptor instead.
func (*PublishRequest) Descriptor() ([]byte, []int) {
	return file_rhino_v1_proto_rawDescGZIP(), []int{3}
}

func (x *PublishRequest) GetTopic() string {
	if x != nil {
		return x.Topic
	}
	return ""
}

func (x *PublishRequest) GetData() []byte {
	if x != nil {
		return x.Data
	}
	return nil
}

func (x *PublishRequest) GetTtl() int32 {
	if x != nil {
		return x.Ttl
	}
	return 0
}

func (x *PublishRequest) GetPersistent() bool {
	if x != nil {
		return x.Persistent
	}
	return false
}

type PublishResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Id    string `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
	Topic string `protobuf:"bytes,2,opt,name=topic,proto3" json:"topic,omitempty"`
	Error *Error `protobuf:"bytes,3,opt,name=error,proto3" json:"error,omitempty"`
}

func (x *PublishResponse) Reset() {
	*x = PublishResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_rhino_v1_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *PublishResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*PublishResponse) ProtoMessage() {}

func (x *PublishResponse) ProtoReflect() protoreflect.Message {
	mi := &file_rhino_v1_proto_msgTypes[4]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use PublishResponse.ProtoReflect.Descriptor instead.
func (*PublishResponse) Descriptor() ([]byte, []int) {
	return file_rhino_v1_proto_rawDescGZIP(), []int{4}
}

func (x *PublishResponse) GetId() string {
	if x != nil {
		return x.Id
	}
	return ""
}

func (x *PublishResponse) GetTopic() string {
	if x != nil {
		return x.Topic
	}
	return ""
}

func (x *PublishResponse) GetError() *Error {
	if x != nil {
		return x.Error
	}
	return nil
}

type AckRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Topic string `protobuf:"bytes,1,opt,name=topic,proto3" json:"topic,omitempty"`
	Id    string `protobuf:"bytes,2,opt,name=id,proto3" json:"id,omitempty"`
}

func (x *AckRequest) Reset() {
	*x = AckRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_rhino_v1_proto_msgTypes[5]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *AckRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*AckRequest) ProtoMessage() {}

func (x *AckRequest) ProtoReflect() protoreflect.Message {
	mi := &file_rhino_v1_proto_msgTypes[5]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use AckRequest.ProtoReflect.Descriptor instead.
func (*AckRequest) Descriptor() ([]byte, []int) {
	return file_rhino_v1_proto_rawDescGZIP(), []int{5}
}

func (x *AckRequest) GetTopic() string {
	if x != nil {
		return x.Topic
	}
	return ""
}

func (x *AckRequest) GetId() string {
	if x != nil {
		return x.Id
	}
	return ""
}

type AckResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Id    string `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
	Topic string `protobuf:"bytes,2,opt,name=topic,proto3" json:"topic,omitempty"`
	Error *Error `protobuf:"bytes,3,opt,name=error,proto3" json:"error,omitempty"`
}

func (x *AckResponse) Reset() {
	*x = AckResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_rhino_v1_proto_msgTypes[6]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *AckResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*AckResponse) ProtoMessage() {}

func (x *AckResponse) ProtoReflect() protoreflect.Message {
	mi := &file_rhino_v1_proto_msgTypes[6]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use AckResponse.ProtoReflect.Descriptor instead.
func (*AckResponse) Descriptor() ([]byte, []int) {
	return file_rhino_v1_proto_rawDescGZIP(), []int{6}
}

func (x *AckResponse) GetId() string {
	if x != nil {
		return x.Id
	}
	return ""
}

func (x *AckResponse) GetTopic() string {
	if x != nil {
		return x.Topic
	}
	return ""
}

func (x *AckResponse) GetError() *Error {
	if x != nil {
		return x.Error
	}
	return nil
}

var File_rhino_v1_proto protoreflect.FileDescriptor

var file_rhino_v1_proto_rawDesc = []byte{
	0x0a, 0x0e, 0x72, 0x68, 0x69, 0x6e, 0x6f, 0x2e, 0x76, 0x31, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x12, 0x08, 0x72, 0x68, 0x69, 0x6e, 0x6f, 0x2e, 0x76, 0x31, 0x22, 0x35, 0x0a, 0x05, 0x45, 0x72,
	0x72, 0x6f, 0x72, 0x12, 0x18, 0x0a, 0x07, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x18, 0x01,
	0x20, 0x01, 0x28, 0x09, 0x52, 0x07, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x12, 0x12, 0x0a,
	0x04, 0x63, 0x6f, 0x64, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x04, 0x63, 0x6f, 0x64,
	0x65, 0x22, 0x2b, 0x0a, 0x13, 0x53, 0x75, 0x62, 0x73, 0x63, 0x72, 0x69, 0x70, 0x74, 0x69, 0x6f,
	0x6e, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x14, 0x0a, 0x05, 0x74, 0x6f, 0x70, 0x69,
	0x63, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x05, 0x74, 0x6f, 0x70, 0x69, 0x63, 0x22, 0x77,
	0x0a, 0x14, 0x53, 0x75, 0x62, 0x73, 0x63, 0x72, 0x69, 0x70, 0x74, 0x69, 0x6f, 0x6e, 0x52, 0x65,
	0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x0e, 0x0a, 0x02, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01,
	0x28, 0x09, 0x52, 0x02, 0x69, 0x64, 0x12, 0x14, 0x0a, 0x05, 0x74, 0x6f, 0x70, 0x69, 0x63, 0x18,
	0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x05, 0x74, 0x6f, 0x70, 0x69, 0x63, 0x12, 0x12, 0x0a, 0x04,
	0x64, 0x61, 0x74, 0x61, 0x18, 0x03, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x04, 0x64, 0x61, 0x74, 0x61,
	0x12, 0x25, 0x0a, 0x05, 0x65, 0x72, 0x72, 0x6f, 0x72, 0x18, 0x04, 0x20, 0x01, 0x28, 0x0b, 0x32,
	0x0f, 0x2e, 0x72, 0x68, 0x69, 0x6e, 0x6f, 0x2e, 0x76, 0x31, 0x2e, 0x45, 0x72, 0x72, 0x6f, 0x72,
	0x52, 0x05, 0x65, 0x72, 0x72, 0x6f, 0x72, 0x22, 0x6c, 0x0a, 0x0e, 0x50, 0x75, 0x62, 0x6c, 0x69,
	0x73, 0x68, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x14, 0x0a, 0x05, 0x74, 0x6f, 0x70,
	0x69, 0x63, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x05, 0x74, 0x6f, 0x70, 0x69, 0x63, 0x12,
	0x12, 0x0a, 0x04, 0x64, 0x61, 0x74, 0x61, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x04, 0x64,
	0x61, 0x74, 0x61, 0x12, 0x10, 0x0a, 0x03, 0x74, 0x74, 0x6c, 0x18, 0x03, 0x20, 0x01, 0x28, 0x05,
	0x52, 0x03, 0x74, 0x74, 0x6c, 0x12, 0x1e, 0x0a, 0x0a, 0x70, 0x65, 0x72, 0x73, 0x69, 0x73, 0x74,
	0x65, 0x6e, 0x74, 0x18, 0x04, 0x20, 0x01, 0x28, 0x08, 0x52, 0x0a, 0x70, 0x65, 0x72, 0x73, 0x69,
	0x73, 0x74, 0x65, 0x6e, 0x74, 0x22, 0x5e, 0x0a, 0x0f, 0x50, 0x75, 0x62, 0x6c, 0x69, 0x73, 0x68,
	0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x0e, 0x0a, 0x02, 0x69, 0x64, 0x18, 0x01,
	0x20, 0x01, 0x28, 0x09, 0x52, 0x02, 0x69, 0x64, 0x12, 0x14, 0x0a, 0x05, 0x74, 0x6f, 0x70, 0x69,
	0x63, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x05, 0x74, 0x6f, 0x70, 0x69, 0x63, 0x12, 0x25,
	0x0a, 0x05, 0x65, 0x72, 0x72, 0x6f, 0x72, 0x18, 0x03, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x0f, 0x2e,
	0x72, 0x68, 0x69, 0x6e, 0x6f, 0x2e, 0x76, 0x31, 0x2e, 0x45, 0x72, 0x72, 0x6f, 0x72, 0x52, 0x05,
	0x65, 0x72, 0x72, 0x6f, 0x72, 0x22, 0x32, 0x0a, 0x0a, 0x41, 0x63, 0x6b, 0x52, 0x65, 0x71, 0x75,
	0x65, 0x73, 0x74, 0x12, 0x14, 0x0a, 0x05, 0x74, 0x6f, 0x70, 0x69, 0x63, 0x18, 0x01, 0x20, 0x01,
	0x28, 0x09, 0x52, 0x05, 0x74, 0x6f, 0x70, 0x69, 0x63, 0x12, 0x0e, 0x0a, 0x02, 0x69, 0x64, 0x18,
	0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x02, 0x69, 0x64, 0x22, 0x5a, 0x0a, 0x0b, 0x41, 0x63, 0x6b,
	0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x0e, 0x0a, 0x02, 0x69, 0x64, 0x18, 0x01,
	0x20, 0x01, 0x28, 0x09, 0x52, 0x02, 0x69, 0x64, 0x12, 0x14, 0x0a, 0x05, 0x74, 0x6f, 0x70, 0x69,
	0x63, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x05, 0x74, 0x6f, 0x70, 0x69, 0x63, 0x12, 0x25,
	0x0a, 0x05, 0x65, 0x72, 0x72, 0x6f, 0x72, 0x18, 0x03, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x0f, 0x2e,
	0x72, 0x68, 0x69, 0x6e, 0x6f, 0x2e, 0x76, 0x31, 0x2e, 0x45, 0x72, 0x72, 0x6f, 0x72, 0x52, 0x05,
	0x65, 0x72, 0x72, 0x6f, 0x72, 0x32, 0xc9, 0x01, 0x0a, 0x05, 0x52, 0x68, 0x69, 0x6e, 0x6f, 0x12,
	0x4c, 0x0a, 0x09, 0x53, 0x75, 0x62, 0x73, 0x63, 0x72, 0x69, 0x62, 0x65, 0x12, 0x1d, 0x2e, 0x72,
	0x68, 0x69, 0x6e, 0x6f, 0x2e, 0x76, 0x31, 0x2e, 0x53, 0x75, 0x62, 0x73, 0x63, 0x72, 0x69, 0x70,
	0x74, 0x69, 0x6f, 0x6e, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x1e, 0x2e, 0x72, 0x68,
	0x69, 0x6e, 0x6f, 0x2e, 0x76, 0x31, 0x2e, 0x53, 0x75, 0x62, 0x73, 0x63, 0x72, 0x69, 0x70, 0x74,
	0x69, 0x6f, 0x6e, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x30, 0x01, 0x12, 0x32, 0x0a,
	0x03, 0x41, 0x63, 0x6b, 0x12, 0x14, 0x2e, 0x72, 0x68, 0x69, 0x6e, 0x6f, 0x2e, 0x76, 0x31, 0x2e,
	0x41, 0x63, 0x6b, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x15, 0x2e, 0x72, 0x68, 0x69,
	0x6e, 0x6f, 0x2e, 0x76, 0x31, 0x2e, 0x41, 0x63, 0x6b, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73,
	0x65, 0x12, 0x3e, 0x0a, 0x07, 0x50, 0x75, 0x62, 0x6c, 0x69, 0x73, 0x68, 0x12, 0x18, 0x2e, 0x72,
	0x68, 0x69, 0x6e, 0x6f, 0x2e, 0x76, 0x31, 0x2e, 0x50, 0x75, 0x62, 0x6c, 0x69, 0x73, 0x68, 0x52,
	0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x19, 0x2e, 0x72, 0x68, 0x69, 0x6e, 0x6f, 0x2e, 0x76,
	0x31, 0x2e, 0x50, 0x75, 0x62, 0x6c, 0x69, 0x73, 0x68, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73,
	0x65, 0x42, 0x8e, 0x01, 0x0a, 0x0c, 0x63, 0x6f, 0x6d, 0x2e, 0x72, 0x68, 0x69, 0x6e, 0x6f, 0x2e,
	0x76, 0x31, 0x42, 0x0c, 0x52, 0x68, 0x69, 0x6e, 0x6f, 0x56, 0x31, 0x50, 0x72, 0x6f, 0x74, 0x6f,
	0x50, 0x01, 0x5a, 0x2f, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x45,
	0x6c, 0x69, 0x6f, 0x65, 0x6e, 0x61, 0x69, 0x46, 0x65, 0x72, 0x72, 0x61, 0x72, 0x69, 0x2f, 0x72,
	0x68, 0x69, 0x6e, 0x6f, 0x2f, 0x67, 0x65, 0x6e, 0x2f, 0x67, 0x6f, 0x3b, 0x72, 0x68, 0x69, 0x6e,
	0x6f, 0x76, 0x31, 0xa2, 0x02, 0x03, 0x52, 0x58, 0x58, 0xaa, 0x02, 0x08, 0x52, 0x68, 0x69, 0x6e,
	0x6f, 0x2e, 0x56, 0x31, 0xca, 0x02, 0x08, 0x52, 0x68, 0x69, 0x6e, 0x6f, 0x5c, 0x56, 0x31, 0xe2,
	0x02, 0x14, 0x52, 0x68, 0x69, 0x6e, 0x6f, 0x5c, 0x56, 0x31, 0x5c, 0x47, 0x50, 0x42, 0x4d, 0x65,
	0x74, 0x61, 0x64, 0x61, 0x74, 0x61, 0xea, 0x02, 0x09, 0x52, 0x68, 0x69, 0x6e, 0x6f, 0x3a, 0x3a,
	0x56, 0x31, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_rhino_v1_proto_rawDescOnce sync.Once
	file_rhino_v1_proto_rawDescData = file_rhino_v1_proto_rawDesc
)

func file_rhino_v1_proto_rawDescGZIP() []byte {
	file_rhino_v1_proto_rawDescOnce.Do(func() {
		file_rhino_v1_proto_rawDescData = protoimpl.X.CompressGZIP(file_rhino_v1_proto_rawDescData)
	})
	return file_rhino_v1_proto_rawDescData
}

var file_rhino_v1_proto_msgTypes = make([]protoimpl.MessageInfo, 7)
var file_rhino_v1_proto_goTypes = []any{
	(*Error)(nil),                // 0: rhino.v1.Error
	(*SubscriptionRequest)(nil),  // 1: rhino.v1.SubscriptionRequest
	(*SubscriptionResponse)(nil), // 2: rhino.v1.SubscriptionResponse
	(*PublishRequest)(nil),       // 3: rhino.v1.PublishRequest
	(*PublishResponse)(nil),      // 4: rhino.v1.PublishResponse
	(*AckRequest)(nil),           // 5: rhino.v1.AckRequest
	(*AckResponse)(nil),          // 6: rhino.v1.AckResponse
}
var file_rhino_v1_proto_depIdxs = []int32{
	0, // 0: rhino.v1.SubscriptionResponse.error:type_name -> rhino.v1.Error
	0, // 1: rhino.v1.PublishResponse.error:type_name -> rhino.v1.Error
	0, // 2: rhino.v1.AckResponse.error:type_name -> rhino.v1.Error
	1, // 3: rhino.v1.Rhino.Subscribe:input_type -> rhino.v1.SubscriptionRequest
	5, // 4: rhino.v1.Rhino.Ack:input_type -> rhino.v1.AckRequest
	3, // 5: rhino.v1.Rhino.Publish:input_type -> rhino.v1.PublishRequest
	2, // 6: rhino.v1.Rhino.Subscribe:output_type -> rhino.v1.SubscriptionResponse
	6, // 7: rhino.v1.Rhino.Ack:output_type -> rhino.v1.AckResponse
	4, // 8: rhino.v1.Rhino.Publish:output_type -> rhino.v1.PublishResponse
	6, // [6:9] is the sub-list for method output_type
	3, // [3:6] is the sub-list for method input_type
	3, // [3:3] is the sub-list for extension type_name
	3, // [3:3] is the sub-list for extension extendee
	0, // [0:3] is the sub-list for field type_name
}

func init() { file_rhino_v1_proto_init() }
func file_rhino_v1_proto_init() {
	if File_rhino_v1_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_rhino_v1_proto_msgTypes[0].Exporter = func(v any, i int) any {
			switch v := v.(*Error); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_rhino_v1_proto_msgTypes[1].Exporter = func(v any, i int) any {
			switch v := v.(*SubscriptionRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_rhino_v1_proto_msgTypes[2].Exporter = func(v any, i int) any {
			switch v := v.(*SubscriptionResponse); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_rhino_v1_proto_msgTypes[3].Exporter = func(v any, i int) any {
			switch v := v.(*PublishRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_rhino_v1_proto_msgTypes[4].Exporter = func(v any, i int) any {
			switch v := v.(*PublishResponse); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_rhino_v1_proto_msgTypes[5].Exporter = func(v any, i int) any {
			switch v := v.(*AckRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_rhino_v1_proto_msgTypes[6].Exporter = func(v any, i int) any {
			switch v := v.(*AckResponse); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_rhino_v1_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   7,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_rhino_v1_proto_goTypes,
		DependencyIndexes: file_rhino_v1_proto_depIdxs,
		MessageInfos:      file_rhino_v1_proto_msgTypes,
	}.Build()
	File_rhino_v1_proto = out.File
	file_rhino_v1_proto_rawDesc = nil
	file_rhino_v1_proto_goTypes = nil
	file_rhino_v1_proto_depIdxs = nil
}
