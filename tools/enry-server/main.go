// enry-server: persistent equivalent of `enry -json <file>` (v2.9.6 printFileAnalysis):
//   language = enry.GetLanguage(path, data[:16MiB]); vendored = enry.IsVendor(path)
// Protocol (stdin): repeated [u32 BE pathLen][path][u32 BE dataLen][data]
// Output (stdout): one JSON line {"language":..,"vendored":..} per request.
package main

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"io"
	"os"

	"github.com/go-enry/go-enry/v2"
)

func readChunk(r io.Reader) ([]byte, error) {
	var n uint32
	if err := binary.Read(r, binary.BigEndian, &n); err != nil {
		return nil, err
	}
	b := make([]byte, n)
	_, err := io.ReadFull(r, b)
	return b, err
}

func main() {
	r := bufio.NewReaderSize(os.Stdin, 1<<20)
	w := bufio.NewWriter(os.Stdout)
	enc := json.NewEncoder(w)
	for {
		p, err := readChunk(r)
		if err != nil {
			return
		}
		d, err := readChunk(r)
		if err != nil {
			return
		}
		// enry CLI default -limit is 16*1024 KB (16 MiB)
		if len(d) > 16*1024*1024 {
			d = d[:16*1024*1024]
		}
		path := string(p)
		lang := enry.GetLanguage(path, d)
		enc.Encode(map[string]interface{}{"language": lang, "vendored": enry.IsVendor(path)})
		w.Flush()
	}
}
