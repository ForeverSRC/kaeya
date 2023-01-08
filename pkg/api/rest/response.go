package rest

const (
	CodeSuccess       = 0
	CodeInternalError = 5000
	CodeBadRequest    = 5001
)

type Response struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data"`
}

func NewSuccessResponse(msg string, data interface{}) Response {
	return Response{
		Code:    CodeSuccess,
		Message: msg,
		Data:    data,
	}
}

func NewErrorResponse(code int, msg string) Response {
	return Response{
		Code:    code,
		Message: msg,
	}
}
