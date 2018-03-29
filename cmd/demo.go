// 用多个协程并发把数据库数据写入到文件中

package cmd

import (
	"fmt"
	"github.com/spf13/cobra"
	"github.com/astaxie/beego/orm"
	"sort"
	"log"
	"os"
	"bufio"
	"sync"
	"time"
	"math"
	"github.com/mitchellh/go-homedir"
	"path/filepath"
)

// demoCmd represents the demo command
var demoCmd = &cobra.Command{
	Use:   "demo",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		log.Println("start")
		initMysql()
		lastId := 0
		count := molDataCount()
		size := math.Ceil(float64(count) / float64(doNum))
		var j = 1
		demoChan = make([]chan int, processNum)
		for i := range demoChan {
			demoChan[i] = make(chan int)
		}
		for {
			for i := 0; i < processNum; i++ {
				go molDataToLog(i, lastId)
			}
			var molIds []int
			for _, chanItem := range demoChan {
				molId := <-chanItem
				molIds = append(molIds, molId)
				if molId != 0 {
					log.Printf("%d/%d", j, int(size))
					j++
				}
			}
			sort.Ints(molIds)
			lastId = molIds[len(molIds)-1]
			//log.Printf("%+v\n", molIds)
			if molIds[0] == 0 {
				break
			}

		}
		time.Sleep(100 * time.Millisecond)
		defer f.Close()
		log.Println("end")

	},
}

var (
	demoChan   []chan int
	doNum      = 1000
	processNum = 10
	w          *bufio.Writer
	l          sync.Mutex
	f          *os.File
)

type searchModata struct {
	MolId      int
	MolName    string
	EnSynonyms string
	ZhSynonyms string
	NameCn     string
	CasNo      string
	Formula    string
}

func init() {
	rootCmd.AddCommand(demoCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// demoCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// demoCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
	initFile()

}

func initFile() {
	var err error
	// Find home directory.
	home, err := homedir.Dir()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	f, err = os.OpenFile(home+string(filepath.Separator)+"logfile.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("file open error : %v", err)
	}
	w = bufio.NewWriter(f) //创建新的 Writer 对象
}

func molDataCount() (count int) {
	qb, _ := orm.NewQueryBuilder("mysql")
	qb = qb.Select("count(mol_id)").
		From("search_moldata")
	sql := qb.String()
	o := orm.NewOrm()

	o.Raw(sql).QueryRow(&count)

	return

}

func molDataToLog(i int, lastId int) {
	var moldata []searchModata
	from := i * doNum
	qb, _ := orm.NewQueryBuilder("mysql")
	qb = qb.Select("mol_id", "mol_name", "en_synonyms", "zh_synonyms", "name_cn", "cas_no", "formula").
		From("search_moldata")

	if lastId > 0 {
		qb = qb.Where("mol_id > ?")
	}
	qb.OrderBy("mol_id").Asc().
		Limit(doNum).Offset(from)
	sql := qb.String()
	//log.Printf("%+v\n",sql)

	o := orm.NewOrm()

	if lastId > 0 {
		o.Raw(sql, lastId).QueryRows(&moldata)
	} else {
		o.Raw(sql).QueryRows(&moldata)
	}

	var molId []int

	for _, item := range moldata {
		molId = append(molId, item.MolId)
	}

	l.Lock()
	for _, item := range moldata {
		str := fmt.Sprintf("%d,%s,%s,%s,%s,%s,%s\n",
			item.MolId, item.NameCn, item.MolName, item.CasNo, item.EnSynonyms, item.ZhSynonyms, item.Formula)
		w.WriteString(str)
	}
	w.Flush()
	l.Unlock()

	if len(molId) > 0 {
		sort.Ints(molId)
		demoChan[i] <- molId[len(molId)-1]
	} else {
		demoChan[i] <- 0
	}

	//defer func() {
	//	close(demoChan[i])
	//}()

}
