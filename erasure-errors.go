package main

import (
	"errors"
	"fmt"
)

//Error definitions
var ErrConfFileNotExist = errors.New("the conf file not exist")
var ErrEmptyData = errors.New("the file to encode is empty")
var ErrDataDirExist = errors.New("data directory already exists")
var ErrTooFewDisks = errors.New("too few survival disks, i.e., k+m < N")
var ErrNotInitialized = errors.New("system not initialized, please initialize with `-mode init` first")
var ErrFileNotFound = errors.New("file not found")
var ErrSurvivalNotEnoughForDecoding = errors.New("the failed block number exceeds fault tolerance, data renders unrecoverable")
var ErrFileIncompleted = errors.New("file hash check fails, file renders incompleted")
var ErrFailModeNotRecognized = errors.New("the fail mode is not recognizable, please specify in \"diskFail\" or \"bitRot\"")
var ErrNegativeReplicateFactor = errors.New("the replicate factor MUST be non-negative")

type DiskError struct {
	diskPath string
	cause    string
}

func (e *DiskError) Error() string {
	return fmt.Sprintf("disk %s is not available for :%s", e.diskPath, e.cause)
}

// errUnexpected - unexpected error, requires manual intervention.
var errUnexpected = StorageErr("unexpected error, please report this issue at https://github.com/minio/minio/issues")

// errCorruptedFormat - corrupted backend format.
var errCorruptedFormat = StorageErr("corrupted backend format, specified disk mount has unexpected previous content")

// errUnformattedDisk - unformatted disk found.
var errUnformattedDisk = StorageErr("unformatted disk found")

// errInconsistentDisk - inconsistent disk found.
var errInconsistentDisk = StorageErr("inconsistent disk found")

// errUnsupporteDisk - when disk does not support O_DIRECT flag.
var errUnsupportedDisk = StorageErr("disk does not support O_DIRECT")

// errDiskFull - cannot create volume or files when disk is full.
var errDiskFull = StorageErr("disk path full")

// errDiskNotDir - cannot use storage disk if its not a directory
var errDiskNotDir = StorageErr("disk is not directory or mountpoint")

// errDiskNotFound - cannot find the underlying configured disk anymore.
var errDiskNotFound = StorageErr("disk not found")

// errFaultyRemoteDisk - remote disk is faulty.
var errFaultyRemoteDisk = StorageErr("remote disk is faulty")

// errFaultyDisk - disk is faulty.
var errFaultyDisk = StorageErr("disk is faulty")

// errDiskAccessDenied - we don't have write permissions on disk.
var errDiskAccessDenied = StorageErr("disk access denied")

// errFileNotFound - cannot find the file.
var errFileNotFound = StorageErr("file not found")

// errFileNotFound - cannot find requested file version.
var errFileVersionNotFound = StorageErr("file version not found")

// errTooManyOpenFiles - too many open files.
var errTooManyOpenFiles = StorageErr("too many open files, please increase 'ulimit -n'")

// errFileNameTooLong - given file name is too long than supported length.
var errFileNameTooLong = StorageErr("file name too long")

// errVolumeExists - cannot create same volume again.
var errVolumeExists = StorageErr("volume already exists")

// errIsNotRegular - not of regular file type.
var errIsNotRegular = StorageErr("not of regular file type")

// errPathNotFound - cannot find the path.
var errPathNotFound = StorageErr("path not found")

// errVolumeNotFound - cannot find the volume.
var errVolumeNotFound = StorageErr("volume not found")

// errVolumeNotEmpty - volume not empty.
var errVolumeNotEmpty = StorageErr("volume is not empty")

// errVolumeAccessDenied - cannot access volume, insufficient permissions.
var errVolumeAccessDenied = StorageErr("volume access denied")

// errFileAccessDenied - cannot access file, insufficient permissions.
var errFileAccessDenied = StorageErr("file access denied")

// errFileCorrupt - file has an unexpected size, or is not readable
var errFileCorrupt = StorageErr("file is corrupted")

// errBitrotHashAlgoInvalid - the algo for bit-rot hash
// verification is empty or invalid.
var errBitrotHashAlgoInvalid = StorageErr("bit-rot hash algorithm is invalid")

// errCrossDeviceLink - rename across devices not allowed.
var errCrossDeviceLink = StorageErr("Rename across devices not allowed, please fix your backend configuration")

// errMinDiskSize - cannot create volume or files when disk size is less than threshold.
var errMinDiskSize = StorageErr("The disk size is less than 900MiB threshold")

// errLessData - returned when less data available than what was requested.
var errLessData = StorageErr("less data available than what was requested")

// errMoreData = returned when more data was sent by the caller than what it was supposed to.
var errMoreData = StorageErr("more data was sent than what was advertised")

// indicates readDirFn to return without further applying the fn()
var errDoneForNow = errors.New("done for now")

// errSkipFile returned by the fn() for readDirFn() when it needs
// to proceed to next entry.
var errSkipFile = errors.New("skip this file")

// StorageErr represents error generated by xlStorage call.
type StorageErr string

func (h StorageErr) Error() string {
	return string(h)
}

// Collection of basic errors.
var baseErrs = []error{
	errDiskNotFound,
	errFaultyDisk,
	errFaultyRemoteDisk,
}

var baseIgnoredErrs = baseErrs

// Is a one place function which converts all os.PathError
// into a more FS object layer friendly form, converts
// known errors into their typed form for top level
// interpretation.
// func osErrToFileErr(err error) error {
// 	if err == nil {
// 		return nil
// 	}
// 	if osIsNotExist(err) {
// 		return errFileNotFound
// 	}
// 	if osIsPermission(err) {
// 		return errFileAccessDenied
// 	}
// 	if isSysErrNotDir(err) || isSysErrIsDir(err) {
// 		return errFileNotFound
// 	}
// 	if isSysErrPathNotFound(err) {
// 		return errFileNotFound
// 	}
// 	if isSysErrTooManyFiles(err) {
// 		return errTooManyOpenFiles
// 	}
// 	if isSysErrHandleInvalid(err) {
// 		return errFileNotFound
// 	}
// 	if isSysErrIO(err) {
// 		return errFaultyDisk
// 	}
// 	if isSysErrInvalidArg(err) {
// 		return errUnsupportedDisk
// 	}
// 	if isSysErrNoSpace(err) {
// 		return errDiskFull
// 	}
// 	return err
// }
