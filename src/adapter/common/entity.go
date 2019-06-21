package common

import (
	"fmt"
	"strings"

	"github.com/gitstliu/go-commonfunctions"
)

type RawLogEntity struct {
	TableName string
	Action    string
	Rows      [][]interface{}
	Header    []string
	HeaderMap map[string]int
	ValueMap  map[string]interface{}
}

func (this *RawLogEntity) ToString() string {
	meta := fmt.Sprintf("TableName:%v Action:%v Header:%v \r\n", this.TableName, this.Action, strings.Join(this.Header, "\t"))
	for _, currRow := range this.Rows {
		meta += fmt.Sprintf("%v \r\n", currRow)
	}
	meta += "************************** \r\n"
	return meta
}

func (this *RawLogEntity) ToJson() (string, error) {
	metaMap := map[string]interface{}{"TableName": this.TableName, "Action": this.Action, "Header": this.Header, "Rows": this.Rows}
	return commonfunctions.ObjectToJson(metaMap)
}
