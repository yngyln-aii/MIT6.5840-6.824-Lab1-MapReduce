package kvraft

type KVMachine struct {
	KV map[string]string
}

func (kv *KVMachine) Get(key string) (string, Err) {
	value, ok := kv.KV[key]
	if !ok {
		return "", ErrNoKey
	}
	return value, OK
}

func (kv *KVMachine) Put(key string, value string) Err {
	kv.KV[key] = value
	return OK
}

func (kv *KVMachine) Append(key string, value string) Err {
	oldValue, ok := kv.KV[key]
	if !ok {
		kv.KV[key] = value
		return OK
	}
	kv.KV[key] = oldValue + value
	return OK
}

func newKVMachine() *KVMachine {
	return &KVMachine{make(map[string]string)}
}
