package main

import (
	"bufio"
	"compress/bzip2"
	"fmt"
	"io"
	"net/http"
	"os"
	"runtime"
	"strings"

	"github.com/coyove/sdss/contrib/bitmap"
	"github.com/coyove/sdss/contrib/cursor"
	"github.com/coyove/sdss/contrib/ngram"
	"github.com/coyove/sdss/types"
)

func downloadWiki(p string) ([]string, error) {
	req, _ := http.NewRequest("GET", "https://dumps.wikimedia.org/zhwiki/20230101/"+p, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	rd := bufio.NewReader(bzip2.NewReader(resp.Body))

	var res []string
	for {
		line, err := rd.ReadString('\n')
		if err != nil {
			if err != io.EOF {
				return nil, err
			}
			break
		}
		parts := strings.SplitN(strings.TrimSpace(line), ":", 3)
		x := parts[2]
		if strings.HasPrefix(x, "Category:") ||
			strings.HasPrefix(x, "WikiProject:") ||
			strings.HasPrefix(x, "Wikipedia:") ||
			strings.HasPrefix(x, "File:") ||
			strings.HasPrefix(x, "Template:") {
			continue
		}
		res = append(res, x)
	}
	return res, nil
}

func main() {
	runtime.GOMAXPROCS(2)
	types.LoadConfig("config.json")
	// dal.InitDB()

	/* for i, p := range strings.Split(`zhwiki-20230101-pages-articles-multistream-index1.txt-p1p187712.bz2
	zhwiki-20230101-pages-articles-multistream-index2.txt-p187713p630160.bz2
	zhwiki-20230101-pages-articles-multistream-index3.txt-p630161p1389648.bz2
	zhwiki-20230101-pages-articles-multistream-index4.txt-p1389649p2889648.bz2
	zhwiki-20230101-pages-articles-multistream-index4.txt-p2889649p3391029.bz2
	zhwiki-20230101-pages-articles-multistream-index5.txt-p3391030p4891029.bz2
	zhwiki-20230101-pages-articles-multistream-index5.txt-p4891030p5596379.bz2
	zhwiki-20230101-pages-articles-multistream-index6.txt-p5596380p7096379.bz2
	zhwiki-20230101-pages-articles-multistream-index6.txt-p7096380p8231694.bz2`, "\n") {
			v, err := downloadWiki(p)
			fmt.Println(p, len(v), err)

			buf := strings.Join(v, "\n")
			ioutil.WriteFile("data"+strconv.Itoa(i), []byte(buf), 0777)
		}*/

	if false {
		f, _ := os.Open("out")
		rd := bufio.NewReader(f)
		data := map[string]bool{}
		for i := 0; ; i++ {
			line, err := rd.ReadString('\n')
			if err != nil {
				break
			}
			if data[line] {
				continue
			}
			data[line] = true
		}
		f.Close()
		f, _ = os.Create("out2")
		for k := range data {
			f.WriteString(k)
		}
		f.Close()
		return
	}

	f, _ := os.Open("out")
	rd := bufio.NewReader(f)
	data := map[int]string{}
	mgr, _ := bitmap.NewManager("bitmap_cache/tags", 1024000, 1*1024*1024*1024)
	for i := 0; ; i++ {
		line, err := rd.ReadString('\n')
		if err != nil {
			break
		}
		line = strings.TrimSpace(line)
		data[i] = line
		//m := ngram.SplitMore(line)
		//for k, v := range ngram.Split(line) {
		//	m[k] = v
		//}
		//h := m.Hashes()
		//if len(h) == 0 {
		//	continue
		//}
		//mgr.Saver().AddAsync(bitmap.Uint64Key(uint64(i)), h)
		if i%100000 == 0 {
			fmt.Println(i)
		}
	}
	mgr.Saver().Close()

	q := "covid"
	h := ngram.SplitMore(q).Hashes()
	h2 := ngram.Split(q).Hashes()

	res := mgr.CollectSimple(cursor.New(), bitmap.Values{Major: h2, Exact: h}, 5000)
	for _, kis := range res {
		fmt.Println(kis, data[int(kis.Key.LowUint64())])
	}
	fmt.Println(len(res))
}
