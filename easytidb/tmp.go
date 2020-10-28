package easytidb

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/types"
)

func GetTableInfoFromTidbStatus(tidbStatusAddr, db, t string) (*model.TableInfo, error) {
	url := fmt.Sprintf("http://%s/schema/%s/%s", tidbStatusAddr, db, t)
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var tableInfo model.TableInfo
	err = json.Unmarshal(body, &tableInfo)
	if err != nil {
		return nil, err
	}
	return &tableInfo, nil
}

func GetColMapFromTableInfo(tableInfo model.TableInfo) map[int64]*types.FieldType {
	colMap := make(map[int64]*types.FieldType, len(tableInfo.Columns))
	for _, col := range tableInfo.Columns {
		colMap[col.ID] = &col.FieldType
	}
	return colMap
}
