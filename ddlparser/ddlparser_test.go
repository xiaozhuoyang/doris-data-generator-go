package ddlparser

import "testing"

func TestParseDDL(t *testing.T) {
	ddl := `
CREATE TABLE t1 (
  id BIGINT NOT NULL,
  name VARCHAR(64),
  amount DECIMAL(18, 2),
  created_at DATETIME(6),
  PRIMARY KEY(id)
)`

	columns, err := Parse(ddl)
	if err != nil {
		t.Fatalf("Parse returned error: %v", err)
	}
	if len(columns) != 4 {
		t.Fatalf("expected 4 columns, got %d", len(columns))
	}
	if columns[0].Name != "id" || columns[0].Type != "BIGINT" {
		t.Fatalf("unexpected first column: %#v", columns[0])
	}
	if columns[1].TypeParams["length"] != 64 {
		t.Fatalf("expected varchar length 64, got %#v", columns[1].TypeParams)
	}
	if columns[2].TypeParams["precision"] != 18 || columns[2].TypeParams["scale"] != 2 {
		t.Fatalf("unexpected decimal params: %#v", columns[2].TypeParams)
	}
	if columns[3].TypeParams["scale"] != 6 {
		t.Fatalf("expected datetime scale 6, got %#v", columns[3].TypeParams)
	}
}

func TestDemoToMapSupportsCSVAndJSON(t *testing.T) {
	csvDemo := "id,name\n1,test"
	csvMap, err := DemoToMap(csvDemo)
	if err != nil {
		t.Fatalf("DemoToMap CSV returned error: %v", err)
	}
	if csvMap["id"] != "1" || csvMap["name"] != "test" {
		t.Fatalf("unexpected csv map: %#v", csvMap)
	}

	jsonDemo := `{"id": 1, "name": "test"}`
	jsonMap, err := DemoToMap(jsonDemo)
	if err != nil {
		t.Fatalf("DemoToMap JSON returned error: %v", err)
	}
	if jsonMap["name"] != "test" {
		t.Fatalf("unexpected json map: %#v", jsonMap)
	}
}
