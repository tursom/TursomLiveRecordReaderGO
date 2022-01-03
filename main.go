package main

import (
	"TursomLiveRecordReaderGO/record"
	"encoding/csv"
	"fmt"
	"github.com/tursom/GoCollections/exceptions"
	"os"
	"time"
)

func main() {
	err := record.LoopRecordFile(".", func(path string) error {
		csvPath := path[0:len(path)-4] + ".csv"
		fmt.Println(csvPath)
		file, err := os.Create(csvPath)
		// todo write danmu to csv
		if err != nil {
			return exceptions.Package(err)
		}
		defer file.Close()

		// write utf8 BOM
		_, err = file.Write([]byte{0xEF, 0xBB, 0xBF})
		if err != nil {
			return exceptions.Package(err)
		}

		csvWriter := csv.NewWriter(file)
		defer csvWriter.Flush()
		//csvWriter.UseCRLF = true
		err = csvWriter.Write([]string{
			"日期", "弹幕", "用户", "uid", "用户等级", "牌子", "牌子主播",
		})
		if err != nil {
			return exceptions.Package(err)
		}

		err = record.ReadRecord(path, func(recordMsg *record.RecordMsg) error {
			danmu := recordMsg.GetDanmu()
			if danmu == nil {
				return nil
			}
			err = csvWriter.Write([]string{
				time.UnixMilli(danmu.Danmu.Metadata.Time).Format("2006-01-02 15:04:05"),
				danmu.Danmu.Danmu,
				danmu.Danmu.UserInfo.Nickname,
				fmt.Sprint(danmu.Danmu.UserInfo.Uid),
				fmt.Sprint(danmu.Danmu.UserLevel.Level),
				danmu.Danmu.BrandInfo.Anchor,
				fmt.Sprint(danmu.Danmu.BrandInfo.Level),
			})
			return exceptions.Package(err)
		})
		return exceptions.Package(err)
	})
	exceptions.Print(err)
}
