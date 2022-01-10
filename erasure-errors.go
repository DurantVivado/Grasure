package grasure

import (
	"errors"
	"fmt"
)

type diskError struct {
	diskPath string
	cause    string
}

func (e *diskError) Error() string {
	return fmt.Sprintf("disk %s is not available for :%s", e.diskPath, e.cause)
}

//Error definitions

var errConfFileNotExist = errors.New("the conf file not exist")

var errEmptyData = errors.New("the file to encode is empty")

var errDataDirExist = errors.New("data directory already exists")

var errTooFewDisksAlive = errors.New("too few survival disks, i.e., k+m < N")

var errNotInitialized = errors.New("system not initialized, please initialize with `-mode init` first")

var errSurvivalNotEnoughForDecoding = errors.New("the failed block number exceeds fault tolerance, data renders unrecoverable")

var errFileIncompleted = errors.New("file hash check fails, file renders incompleted")

var errFileNotFound = errors.New("file not found")

var errFailModeNotRecognized = errors.New("the fail mode is not recognizable, please specify in \"diskFail\" or \"bitRot\"")

var errNegativeReplicateFactor = errors.New("the replicate factor MUST be non-negative")

var errNotEnoughBackupForRecovery = errors.New("not enough disk for recovery, needs more backup devices")

var errFileBlobNotFound = errors.New("file blob not found. please try to read it")

var errDiskNumTooLarge = errors.New("diskNum is larger than provided")

var errTooFewBlockAliveInStripe = errors.New("not enough blocks for reading in a stripe")

var errPartInfoNotFound = errors.New("partition info not found")

var errIoStatNotFound = errors.New("io status await or svctm not found")

// errUnexpected - unexpected error, requires manual intervention.
var errUnexpected = storageErr("unexpected error, please report this issue at https://github.com/minio/minio/issues")

// errCorruptedFormat - corrupted backend format.
var errCorruptedFormat = storageErr("corrupted backend format, specified disk mount has unexpected previous content")

// errUnformattedDisk - unformatted disk found.
var errUnformattedDisk = storageErr("unformatted disk found")

// errInconsistentDisk - inconsistent disk found.
var errInconsistentDisk = storageErr("inconsistent disk found")

// errUnsupporteDisk - when disk does not support O_DIRECT flag.
var errUnsupportedDisk = storageErr("disk does not support O_DIRECT")

// errDiskFull - cannot create volume or files when disk is full.
var errDiskFull = storageErr("disk path full")

// errDiskNotDir - cannot use storage disk if its not a directory
var errDiskNotDir = storageErr("disk is not directory or mountpoint")

// errDiskNotFound - cannot find the underlying configured disk anymore.
var errDiskNotFound = storageErr("disk not found")

// errFaultyRemoteDisk - remote disk is faulty.
var errFaultyRemoteDisk = storageErr("remote disk is faulty")

// errFaultyDisk - disk is faulty.
var errFaultyDisk = storageErr("disk is faulty")

// errDiskAccessDenied - we don't have write permissions on disk.
var errDiskAccessDenied = storageErr("disk access denied")

// errFileNotFound - cannot find the file.

// errFileNotFound - cannot find requested file version.
var errFileVersionNotFound = storageErr("file version not found")

// errTooManyOpenFiles - too many open files.
var errTooManyOpenFiles = storageErr("too many open files, please increase 'ulimit -n'")

// errFileNameTooLong - given file name is too long than supported length.
var errFileNameTooLong = storageErr("file name too long")

// errVolumeExists - cannot create same volume again.
var errVolumeExists = storageErr("volume already exists")

// errIsNotRegular - not of regular file type.
var errIsNotRegular = storageErr("not of regular file type")

// errPathNotFound - cannot find the path.
var errPathNotFound = storageErr("path not found")

// errVolumeNotFound - cannot find the volume.
var errVolumeNotFound = storageErr("volume not found")

// errVolumeNotEmpty - volume not empty.
var errVolumeNotEmpty = storageErr("volume is not empty")

// errVolumeAccessDenied - cannot access volume, insufficient permissions.
var errVolumeAccessDenied = storageErr("volume access denied")

// errFileAccessDenied - cannot access file, insufficient permissions.
var errFileAccessDenied = storageErr("file access denied")

// errFileCorrupt - file has an unexpected size, or is not readable
var errFileCorrupt = storageErr("file is corrupted")

// errBitrotHashAlgoInvalid - the algo for bit-rot hash
// verification is empty or invalid.
var errBitrotHashAlgoInvalid = storageErr("bit-rot hash algorithm is invalid")

// errCrossDeviceLink - rename across devices not allowed.
var errCrossDeviceLink = storageErr("Rename across devices not allowed, please fix your backend configuration")

// errMinDiskSize - cannot create volume or files when disk size is less than threshold.
var errMinDiskSize = storageErr("The disk size is less than 900MiB threshold")

// errLessData - returned when less data available than what was requested.
var errLessData = storageErr("less data available than what was requested")

// errMoreData = returned when more data was sent by the caller than what it was supposed to.
var errMoreData = storageErr("more data was sent than what was advertised")

// indicates readDirFn to return without further applying the fn()
var errDoneForNow = errors.New("done for now")

// errSkipFile returned by the fn() for readDirFn() when it needs
// to proceed to next entry.
var errSkipFile = errors.New("skip this file")

// storageErr represents error generated by xlStorage call.
type storageErr string

func (h storageErr) error() string {
	return string(h)
}

// Collection of basic errors.
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
