package main

import "encoding/json"
import "fmt"
import "log"
import "os"
import "regexp"
import "strings"
import "sync"
import "time"

import "github.com/aws/aws-sdk-go/aws"
import "github.com/aws/aws-sdk-go/aws/awserr"
import "github.com/aws/aws-sdk-go/aws/credentials"
import "github.com/aws/aws-sdk-go/aws/session"
import "github.com/aws/aws-sdk-go/service/s3"

type ControlFile struct {
	ConcurrentWorkerThreads uint
	SyncInfos               []SyncInfo
	WorkerPoolBufferSize    uint
}

type controlInfo struct {
	exclude  []*regexp.Regexp
	include  []*regexp.Regexp
	syncInfo SyncInfo
}

type SyncInfo struct {
	AccessKeyId      string
	AccessKeySecret  string
	BaseDir          string
	BucketName       string
	BucketPrefix     string
	DryRun           bool
	Exclude          []string
	Include          []string
	Region           string
	StorageClass     string
	SuppressWarnings bool
}

type fileInfo struct {
	lastModified time.Time
	localPath    string
	remotePath   string
	size         int64
}

func main() {
	if len(os.Args) != 2 {
		log.Fatalln("Usage: foldersync <path to control file>")
	}
	controlFile := readControlFile(os.Args[1])
	errs := validateControlFile(controlFile)

	controlInfos := make([]controlInfo, len(controlFile.SyncInfos))
	for i, syncInfo := range controlFile.SyncInfos {
		syncInfo := syncInfo // Properly capture variable
		controlInfos[i].syncInfo = syncInfo
		controlInfos[i].exclude = make([]*regexp.Regexp, len(syncInfo.Exclude))
		for j, exclude := range syncInfo.Exclude {
			regex, err := regexp.Compile(exclude)
			if err != nil {
				errs = append(errs, fmt.Errorf("Could not parse exclude pattern %d in sync directive %d: %s", j+1, i+1, err.Error()))
			}
			controlInfos[i].exclude[j] = regex
		}
		controlInfos[i].include = make([]*regexp.Regexp, len(syncInfo.Include))
		for j, include := range syncInfo.Include {
			regex, err := regexp.Compile(include)
			if err != nil {
				errs = append(errs, fmt.Errorf("Could not parse include pattern %d in sync directive %d: %s", j+1, i+1, err.Error()))
			}
			controlInfos[i].include[j] = regex
		}
	}

	if len(errs) != 0 {
		for _, err := range errs {
			log.Println(err.Error())
		}
		os.Exit(1)
	}

	workerChannelWaitGroup, workerChannel := makeWorkerPool(controlFile.ConcurrentWorkerThreads, controlFile.WorkerPoolBufferSize)

	var syncInfoWaitGroup sync.WaitGroup
	syncInfoWaitGroup.Add(len(controlFile.SyncInfos))

	for _, control := range controlInfos {
		control := control // Properly capture variable
		syncInfo := control.syncInfo
		go func() {
			log.Printf("Syncing %s to %s", syncInfo.BaseDir, syncInfo.BucketName)
			syncLocalDirToAwsBucket(workerChannel, control)
			log.Printf("Done syncing %s to %s", syncInfo.BaseDir, syncInfo.BucketName)
			syncInfoWaitGroup.Done()
		}()
	}
	syncInfoWaitGroup.Wait()

	close(workerChannel)
	workerChannelWaitGroup.Wait()

	log.Printf("Sync complete")
}

func readControlFile(path string) ControlFile {
	controlFile, err := os.Open(path)
	if err != nil {
		log.Fatalf("ERROR: could not open control file %s: %s", path, err.Error())
	}
	defer controlFile.Close()

	decoder := json.NewDecoder(controlFile)
	var controlInfo ControlFile
	err = decoder.Decode(&controlInfo)
	if err != nil {
		log.Fatalf("ERROR: could not read control file %s: %s", path, err.Error())
	}

	return controlInfo
}

func validateControlFile(controlInfo ControlFile) []error {
	errs := make([]error, 0)

	if controlInfo.ConcurrentWorkerThreads == 0 {
		errs = append(errs, fmt.Errorf("Concurrent worker threads must be greater than 0"))
	}
	if controlInfo.WorkerPoolBufferSize == 0 {
		errs = append(errs, fmt.Errorf("Worker pool buffer size must be greater than 0"))
	}

	for i, syncInfo := range controlInfo.SyncInfos {
		if syncInfo.AccessKeyId == "" {
			errs = append(errs, fmt.Errorf("AccessKeyId in sync directive %d is required and must not be empty", i+1))
		}
		if syncInfo.AccessKeySecret == "" {
			errs = append(errs, fmt.Errorf("AccessKeySecret in sync directive %d is required and must not be empty", i+1))
		}
		if syncInfo.BaseDir == "" {
			errs = append(errs, fmt.Errorf("BaseDir in sync directive %d is required and must not be empty", i+1))
		} else {
			fileInfo, err := os.Stat(syncInfo.BaseDir)
			if err != nil {
				errs = append(errs, fmt.Errorf("BaseDir in sync directive %d cannot be opened: %s", i+1, err.Error()))
			} else if !fileInfo.IsDir() {
				errs = append(errs, fmt.Errorf("BaseDir in sync directive %d is not a directory", i+1, err.Error()))
			}
		}
		if syncInfo.BucketName == "" {
			errs = append(errs, fmt.Errorf("BucketName in sync directive %d is required and must not be empty", i+1))
		}
		if syncInfo.Region == "" {
			errs = append(errs, fmt.Errorf("Region in sync directive %d is required and must not be empty", i+1))
		}
		if syncInfo.StorageClass == "" {
			errs = append(errs, fmt.Errorf("StorageClass in sync directive %d is required and must not be empty", i+1))
		}
	}
	return errs
}

func syncLocalDirToAwsBucket(workerChannel chan<- func(), control controlInfo) {
	syncInfo := control.syncInfo
	sess := session.Must(session.NewSession(&aws.Config{Credentials: credentials.NewStaticCredentials(syncInfo.AccessKeyId, syncInfo.AccessKeySecret, ""), Region: &syncInfo.Region}))
	service := s3.New(sess)

	remoteFileMapChannel := make(chan map[string]fileInfo)
	workerChannel <- func() {
		remoteFileMap := readAwsBucket(service, syncInfo)
		remoteFileMapChannel <- remoteFileMap
	}

	localFileMapChannel := make(chan map[string]fileInfo)
	go func() {
		localFileMap := readFileSystem(syncInfo)
		localFileMapChannel <- localFileMap
	}()

	remoteFileMap := <-remoteFileMapChannel
	localFileMap := <-localFileMapChannel

	var waitGroup sync.WaitGroup
	waitGroup.Add(2)

	go func() {
		printMissingLocalFileWarnings(syncInfo, localFileMap, remoteFileMap)
		waitGroup.Done()
	}()

	go func() {
		uploadMissingFilesToAws(control, localFileMap, remoteFileMap, service, workerChannel, &waitGroup)
		waitGroup.Done()
	}()

	waitGroup.Wait()
}

func printMissingLocalFileWarnings(control SyncInfo, localFileMap map[string]fileInfo, remoteFileMap map[string]fileInfo) {
	for _, remoteFile := range remoteFileMap {
		_, ok := localFileMap[remoteFile.remotePath]
		if !ok && !control.SuppressWarnings {
			log.Printf("WARNING: remote file %s in bucket %s not present locally in %s", remoteFile.remotePath, control.BucketName, control.BaseDir)
		}
	}
}

func uploadMissingFilesToAws(control controlInfo, localFileMap map[string]fileInfo, remoteFileMap map[string]fileInfo, service *s3.S3, workerChannel chan<- func(), waitGroup *sync.WaitGroup) {
	for _, localFile := range localFileMap {
		remoteFile, ok := remoteFileMap[localFile.remotePath]
		if !ok || remoteFile.size != localFile.size || localFile.lastModified.After(remoteFile.lastModified) {
			localFile := localFile // Properly capture variable
			waitGroup.Add(1)
			workerChannel <- func() {
				putObject(control, localFile, service)
				waitGroup.Done()
			}
		}
	}
}

func makeWorkerPool(poolSize uint, bufferSize uint) (*sync.WaitGroup, chan<- func()) {
	var waitGroup sync.WaitGroup
	channel := make(chan func(), bufferSize)
	waitGroup.Add(int(poolSize))
	for i := uint(0); i < poolSize; i++ {
		go func() {
			for task := range channel {
				task()
			}
			waitGroup.Done()
		}()
	}
	return &waitGroup, channel
}

func putObject(control controlInfo, localFile fileInfo, service *s3.S3) {
	syncInfo := control.syncInfo
	var builder strings.Builder
	builder.WriteString(syncInfo.BaseDir)
	builder.WriteRune(os.PathSeparator)
	builder.WriteString(localFile.localPath)
	pathToOpen := builder.String()

	include := true
	for i := 0; i < len(control.include) && include; i++ {
		include = control.include[i].MatchString(localFile.localPath)
	}
	for i := 0; i < len(control.exclude) && include; i++ {
		include = !control.exclude[i].MatchString(localFile.localPath)
	}

	if include {
		remotePath := localFile.remotePath
		log.Printf("Starting upload of %s to %s in bucket %s", pathToOpen, remotePath, syncInfo.BucketName)

		file, err := os.Open(pathToOpen)
		if err != nil {
			log.Printf("ERROR: could not upload file %s to key %s in bucket %s.  Open failed: %s", pathToOpen, remotePath, syncInfo.BucketName, err.Error())
			return
		}
		defer file.Close()

		if !syncInfo.DryRun {
			input := s3.PutObjectInput{Body: file, Bucket: &syncInfo.BucketName, ContentLength: &localFile.size, Key: &remotePath, StorageClass: &syncInfo.StorageClass}
			_, err = service.PutObject(&input)
			if err != nil {
				log.Printf("ERROR: upload to bucket %s, key %s", syncInfo.BucketName, remotePath)
				for err != nil {
					if awsError, ok := err.(awserr.Error); ok {
						log.Printf("ERROR: %s: %s", awsError.Code(), awsError.Message())
						err = awsError.OrigErr()
					} else {
						log.Printf(err.Error())
						err = nil
					}
				}
				log.Printf("ERROR: exiting AWS upload to bucket %s due to errors", syncInfo.BucketName)
			}
		} else {
			log.Printf("Skipping actual upload of %s to %s in bucket %s: dry-run enabled", pathToOpen, remotePath, syncInfo.BucketName)
		}

		log.Printf("Completed upload of %s to %s in bucket %s", pathToOpen, remotePath, syncInfo.BucketName)
	}
}

func readAwsBucket(service *s3.S3, control SyncInfo) map[string]fileInfo {
	uploadedMap := make(map[string]fileInfo)
	more := true
	var continuationToken *string = nil
	for more {
		log.Printf("Starting request to %s", control.BucketName)
		input := s3.ListObjectsV2Input{Bucket: &control.BucketName, ContinuationToken: continuationToken}
		output, err := service.ListObjectsV2(&input)
		if err != nil {
			log.Printf("ERROR: list bucket %s", control.BucketName)
			for err != nil {
				if awsError, ok := err.(awserr.Error); ok {
					log.Printf("ERROR: %s: %s", awsError.Code(), awsError.Message())
					err = awsError.OrigErr()
				} else {
					log.Printf(err.Error())
					err = nil
				}
			}
			log.Printf("ERROR: exiting AWS read of bucket %s due to errors", control.BucketName)
			return uploadedMap
		}
		more = (*output).NextContinuationToken != nil
		continuationToken = (*output).NextContinuationToken
		log.Printf("Got %d results, continuation token present %t while reading bucket %s", *(*output).KeyCount, more, control.BucketName)

		for _, v := range (*output).Contents {
			localPath := strings.ReplaceAll(*(*v).Key, "/", string(os.PathSeparator))
			item := fileInfo{lastModified: *(*v).LastModified, localPath: localPath, remotePath: *(*v).Key, size: *(*v).Size}
			uploadedMap[item.remotePath] = item
		}
	}
	return uploadedMap
}

func readFileSystem(control SyncInfo) map[string]fileInfo {
	fileMap := make(map[string]fileInfo)
	readFileSystemInternal(control, "", fileMap)
	return fileMap
}

func readFileSystemInternal(control SyncInfo, relativePath string, fileMap map[string]fileInfo) {
	var pathToOpen string
	if relativePath != "" {
		var builder strings.Builder
		builder.WriteString(control.BaseDir)
		builder.WriteRune(os.PathSeparator)
		builder.WriteString(relativePath)
		pathToOpen = builder.String()
	} else {
		pathToOpen = control.BaseDir
	}

	file, err := os.Open(pathToOpen)
	if err != nil {
		log.Printf("ERROR: cannot open local file %s: ", pathToOpen, err.Error())
		log.Printf("ERROR: exiting local file system read of %s due to errors", pathToOpen)
		return
	}
	defer file.Close()

	files, err := file.Readdir(0)
	if err != nil {
		log.Printf("ERROR: cannot read directory %s: ", pathToOpen, err.Error())
		log.Printf("ERROR: exiting local file system read of %s due to errors", pathToOpen)
		return
	}

	for _, f := range files {
		var currentItemRelativePath string
		if relativePath != "" {
			var builder strings.Builder
			builder.WriteString(relativePath)
			builder.WriteRune(os.PathSeparator)
			builder.WriteString(f.Name())
			currentItemRelativePath = builder.String()
		} else {
			currentItemRelativePath = f.Name()
		}
		if f.IsDir() {
			readFileSystemInternal(control, currentItemRelativePath, fileMap)
		} else {
			remotePath := control.BucketPrefix + strings.ReplaceAll(currentItemRelativePath, string(os.PathSeparator), "/")
			item := fileInfo{lastModified: f.ModTime(), localPath: currentItemRelativePath, remotePath: remotePath, size: f.Size()}
			fileMap[item.remotePath] = item
		}
	}
}
