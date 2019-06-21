package handler

import (
	"adapter/common"
	"output"

	"github.com/gitstliu/log4go"
	"github.com/siddontang/go-mysql/canal"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
)

type CommonEventHandler struct {
	canal.DummyEventHandler
	CurrOutput *output.Output
	//	PosSync    chan *mysql.Position
}

func (this *CommonEventHandler) OnRow(e *canal.RowsEvent) error {
	log4go.Debug("OnRow")
	entity := &common.RawLogEntity{}
	entity.Action = e.Action
	entity.Rows = e.Rows
	entity.TableName = e.Table.Name
	entity.Header = []string{}
	entity.HeaderMap = map[string]int{}
	entity.ValueMap = map[string]interface{}{}

	for columnIndex, currColumn := range e.Table.Columns {
		entity.Header = append(entity.Header, currColumn.Name)
		entity.HeaderMap[currColumn.Name] = columnIndex
		entity.ValueMap[currColumn.Name] = e.Rows[len(e.Rows)-1][columnIndex]
	}
	log4go.Debug(entity)
	this.CurrOutput.Write(entity)

	return nil
}

func (this *CommonEventHandler) String() string {
	return "MyEventHandler"
}

func (this *CommonEventHandler) OnRotate(e *replication.RotateEvent) error {
	this.CurrOutput.Write(&mysql.Position{Name: string(e.NextLogName), Pos: uint32(e.Position)})
	return nil
}

func (this *CommonEventHandler) OnTableChanged(schema string, table string) error {
	return nil
}

func (this *CommonEventHandler) OnDDL(nextPos mysql.Position, queryEvent *replication.QueryEvent) error {
	return nil
}

func (this *CommonEventHandler) OnXID(mysql.Position) error {
	return nil
}

func (this *CommonEventHandler) OnGTID(mysql.GTIDSet) error {
	return nil
}

func (this *CommonEventHandler) OnPosSynced(mysql.Position, bool) error {
	return nil
}
