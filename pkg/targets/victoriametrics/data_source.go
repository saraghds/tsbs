package victoriametrics

import (
	"bufio"
	"fmt"
	"log"

	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/data/usecases/common"
)

type fileDataSource struct {
	scanner *bufio.Scanner
}

func (f fileDataSource) NextItem() data.LoadedPoint {
	fmt.Println("NextItem")
	ok := f.scanner.Scan()
	if !ok && f.scanner.Err() == nil { // nothing scanned & no error = EOF
		fmt.Println("NextItem !ok && f.scanner.Err() == nil ")
		return data.LoadedPoint{}
	} else if !ok {
		fmt.Println("NextItem !ok")
		log.Fatalf("scan error: %v", f.scanner.Err())
	}
	fmt.Println("NextItem NewLoadedPoint")
	return data.NewLoadedPoint(f.scanner.Bytes())
}

func (f fileDataSource) Headers() *common.GeneratedDataHeaders {
	return nil
}

type decoder struct {
	scanner *bufio.Scanner
}
