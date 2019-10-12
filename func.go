package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/Sugi275/serless_get-image-metadata/loglib"
	_ "github.com/mattn/go-oci8"
)

const (
	envBucketName        = "OCI_BUCKETNAME"
	envSourceRegion      = "OCI_SOURCE_REGION"
	envTenancyName       = "OCI_TENANCY_NAME"
	envOracleUsername    = "ORACLE_USERNAME"
	envOraclePassword    = "ORACLE_PASSWORD"
	envOracleServiceName = "ORACLE_SERVICENAME"
	actionTypeCreate     = "com.oraclecloud.objectstorage.createobject"
	actionTypeUpdate     = "com.oraclecloud.objectstorage.updateobject"
	actionTypeDelete     = "com.oraclecloud.objectstorage.deleteobject"
)

// ImageList Image を複数格納するstruck
type ImageList struct {
	Object string  `json:"object"`
	Type   string  `json:"type"`
	Total  int     `json:"total"`
	Data   []Image `json:"data"`
}

//Image Image
type Image struct {
	ID          string    `json:"id"`
	Object      string    `json:"object"`
	Imagename   string    `json:"imagename"`
	Detail      string    `json:"detail"`
	ImageURL    string    `json:"image_url"`
	Owner       string    `json:"owner"`
	CreatedDate time.Time `json:"created_date"`
	Deleted     int       `json:"deleted"`
}

func main() {
	// fdk.Handle(fdk.HandlerFunc(fnMain))

	// ------- local development ---------
	reader := os.Stdin
	writer := os.Stdout
	fnMain(context.TODO(), reader, writer)
}

func fnMain(ctx context.Context, in io.Reader, out io.Writer) {
	loglib.InitSugar()
	defer loglib.Sugar.Sync()

	imageList, err := getImageList()
	if err != nil {
		loglib.Sugar.Error(err)
		return
	}

	json.NewEncoder(out).Encode(&imageList)

	return
}

func newImageListConst() ImageList {
	var imageList ImageList
	var images []Image

	imageList = ImageList{
		Object: "list",
		Type:   "image",
		Total:  0,
		Data:   images,
	}

	return imageList
}

func getImageList() (ImageList, error) {
	imageList := newImageListConst()

	dsn, err := getDSN()
	if err != nil {
		loglib.Sugar.Error(err)
		return imageList, err
	}

	db, err := sql.Open("oci8", dsn)
	if err != nil {
		loglib.Sugar.Error(err)
		return imageList, err
	}

	defer db.Close()

	imageList, err = selectImage(db, imageList)
	if err != nil {
		loglib.Sugar.Error(err)
		return imageList, err
	}

	return imageList, nil
}

func selectImage(db *sql.DB, imageList ImageList) (ImageList, error) {
	query := "SELECT id, ImageName, Detail, ImageURL, UserName, CREATE_DATE, DELETED FROM (SELECT * FROM IMAGES ORDER BY CREATE_DATE DESC) A WHERE ROWNUM <= 10"

	var rows *sql.Rows
	rows, err := db.Query(query)
	defer rows.Close()

	if err != nil {
		loglib.Sugar.Error(err)
		return imageList, err
	}

	for rows.Next() {
		var id sql.NullString
		var imagename sql.NullString
		var detail sql.NullString
		var imageurl sql.NullString
		var userName sql.NullString
		var createDate time.Time
		var deleted int

		err := rows.Scan(&id, &imagename, &detail, &imageurl, &userName, &createDate, &deleted)
		if err != nil {
			loglib.Sugar.Error(err)
			return imageList, err
		}
		fmt.Printf("id:%s, imagename:%s, detail:%s, imageurl:%s, userName:%s, createDate:%s deleted:%b\n",
			validNull(id), validNull(imagename), validNull(detail), validNull(imageurl), validNull(userName), createDate, deleted)

		image := Image{
			ID:          validNull(id),
			Object:      "Image",
			Imagename:   validNull(imagename),
			Detail:      validNull(detail),
			ImageURL:    validNull(imageurl),
			Owner:       validNull(userName),
			CreatedDate: createDate,
			Deleted:     deleted,
		}

		imageList.Data = append(imageList.Data, image)
		imageList.Total = imageList.Total + 1
	}

	loglib.Sugar.Infof("Successful. Select Metadata")

	return imageList, nil
}

func validNull(nullString sql.NullString) string {
	if nullString.Valid {
		return nullString.String
	} else {
		return ""
	}
}

func getDSN() (string, error) {
	oracleUsername, ok := os.LookupEnv(envOracleUsername)
	if !ok {
		err := fmt.Errorf("can not read environment variable %s", envOracleUsername)
		loglib.Sugar.Error(err)
		return "", err
	}

	oraclePassword, ok := os.LookupEnv(envOraclePassword)
	if !ok {
		err := fmt.Errorf("can not read environment variable %s", envOraclePassword)
		loglib.Sugar.Error(err)
		return "", err
	}

	oracleServiceName, ok := os.LookupEnv(envOracleServiceName)
	if !ok {
		err := fmt.Errorf("can not read environment variable %s", envOracleServiceName)
		loglib.Sugar.Error(err)
		return "", err
	}

	connect := oracleUsername + "/" + oraclePassword + "@" + oracleServiceName
	secretedConnect := oracleUsername + "/secret@" + oracleServiceName

	loglib.Sugar.Infof("Generated connect:" + secretedConnect)

	return connect, nil
}
