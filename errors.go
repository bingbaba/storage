package storage

import (
	"fmt"
	"net/http"
)

const (
	ErrCodeKeyNotFound int = iota + 1
	ErrCodeKeyExists
	ErrCodeResourceVersionConflicts
	ErrCodeInvalidObj
	ErrCodeUnreachable
	ErrCodeBadRequest
)

var errCodeToMessage = map[int]string{
	ErrCodeKeyNotFound:              "KeyNotFound",
	ErrCodeKeyExists:                "KeyExists",
	ErrCodeResourceVersionConflicts: "ResourceVersionConflicts",
	ErrCodeInvalidObj:               "InvalidObject",
	ErrCodeUnreachable:              "ServerUnreachable",
	ErrCodeBadRequest:               "BadRequest",
}

func NewKeyNotFoundError(key string, rv int64) *StorageError {
	return &StorageError{
		Code:            ErrCodeKeyNotFound,
		Key:             key,
		ResourceVersion: rv,
	}
}

func NewKeyExistsError(key string, rv int64) *StorageError {
	return &StorageError{
		Code:            ErrCodeKeyExists,
		Key:             key,
		ResourceVersion: rv,
	}
}

func NewResourceVersionConflictsError(key string, rv int64) *StorageError {
	return &StorageError{
		Code:            ErrCodeResourceVersionConflicts,
		Key:             key,
		ResourceVersion: rv,
	}
}

func NewUnreachableError(key string, rv int64) *StorageError {
	return &StorageError{
		Code:            ErrCodeUnreachable,
		Key:             key,
		ResourceVersion: rv,
	}
}

func NewInvalidObjError(key, msg string) *StorageError {
	return &StorageError{
		Code:               ErrCodeInvalidObj,
		Key:                key,
		AdditionalErrorMsg: msg,
	}
}

func NewBadRequestError(msg string) *StorageError {
	return &StorageError{
		Code:               ErrCodeBadRequest,
		AdditionalErrorMsg: msg,
	}
}

type StorageError struct {
	Code               int
	Key                string
	ResourceVersion    int64
	AdditionalErrorMsg string
}

func (e *StorageError) Error() string {
	return fmt.Sprintf("StorageError: %s, Code: %d, Key: %s, ResourceVersion: %d, AdditionalErrorMsg: %s",
		errCodeToMessage[e.Code], e.Code, e.Key, e.ResourceVersion, e.AdditionalErrorMsg)
}

func GetErrMesage(err error) string {
	if e, ok := err.(*StorageError); ok {
		return errCodeToMessage[e.Code]
	} else {
		return err.Error()
	}
}

// IsNotFound returns true if and only if err is "key" not found error.
func IsNotFound(err error) bool {
	return isErrCode(err, ErrCodeKeyNotFound)
}

// IsNodeExist returns true if and only if err is an node already exist error.
func IsNodeExist(err error) bool {
	return isErrCode(err, ErrCodeKeyExists)
}

// IsUnreachable returns true if and only if err indicates the server could not be reached.
func IsUnreachable(err error) bool {
	return isErrCode(err, ErrCodeUnreachable)
}

// IsConflict returns true if and only if err is a write conflict.
func IsConflict(err error) bool {
	return isErrCode(err, ErrCodeResourceVersionConflicts)
}

// IsInvalidObj returns true if and only if err is invalid error
func IsInvalidObj(err error) bool {
	return isErrCode(err, ErrCodeInvalidObj)
}

// IsBadRequest
func IsBadRequest(err error) bool {
	return isErrCode(err, ErrCodeBadRequest)
}

func isErrCode(err error, code int) bool {
	if err == nil {
		return false
	}
	if e, ok := err.(*StorageError); ok {
		return e.Code == code
	}
	return false
}

// InternalError is generated when an error occurs in the storage package, i.e.,
// not from the underlying storage backend (e.g., etcd).
type InternalError struct {
	Reason string
}

func (e InternalError) Error() string {
	return e.Reason
}

// IsInternalError returns true if and only if err is an InternalError.
func IsInternalError(err error) bool {
	_, ok := err.(InternalError)
	return ok
}

func IsTimeoutError(err error) bool {
	if err.Error() == "context deadline exceeded" {
		return true
	} else {
		return false
	}
}

func NewInternalError(reason string) InternalError {
	return InternalError{reason}
}

func NewInternalErrorf(format string, a ...interface{}) InternalError {
	return InternalError{fmt.Sprintf(format, a)}
}

func ParseToHttpError(err error) (code int, msg, detail string) {
	// storage error
	if e, ok := err.(*StorageError); ok {
		switch e.Code {
		case ErrCodeKeyNotFound:
			return http.StatusNotFound, errCodeToMessage[ErrCodeKeyNotFound], err.Error()
		case ErrCodeKeyExists:
			return http.StatusConflict, errCodeToMessage[ErrCodeKeyExists], err.Error()
		case ErrCodeResourceVersionConflicts:
			return http.StatusConflict, errCodeToMessage[ErrCodeResourceVersionConflicts], err.Error()
		case ErrCodeInvalidObj:
			return http.StatusInternalServerError, errCodeToMessage[ErrCodeInvalidObj], err.Error()
		case ErrCodeUnreachable:
			return http.StatusNotFound, errCodeToMessage[ErrCodeUnreachable], err.Error()
		case ErrCodeBadRequest:
			return http.StatusBadRequest, errCodeToMessage[ErrCodeBadRequest], err.Error()
		default:
			return http.StatusInternalServerError, err.Error(), err.Error()
		}
	}

	//IsInternalError
	if IsInternalError(err) {
		return http.StatusInternalServerError, "internal error", err.Error()
	} else if IsTimeoutError(err) {
		return http.StatusInternalServerError, "request timeout", err.Error()
	}

	return http.StatusInternalServerError, "unknown error", err.Error()
}
