package filetype

type TableType int8

const (
	Csv TableType = iota
	Tsv
)

type ContentType int8

const (
	Unknown ContentType = iota
	IidOnly
	FidAndIid
)

func (t TableType) ToString() string {
	switch t {
	case Csv:
		return ".csv"
	case Tsv:
		return ".tsv"
	default:
		return ""
	}
}

func (t TableType) Separator() string {
	switch t {
	case Csv:
		return ","
	case Tsv:
		return "\t"
	default:
		return ""
	}
}
