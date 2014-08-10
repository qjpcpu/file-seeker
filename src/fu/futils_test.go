package fu

import(
	"os"
	"testing"
	"strings"
)

func TestGetline(t *testing.T){
	filename:="./futils.go"
	file,_:=os.Open(filename)
	_,text,err:=Getline(25,file)
	if err!=nil{
		t.Fatal("open file fail",err)
	}
	if !strings.Contains(text,"bufio"){
		t.Fatal("get:"+text)
	}
	_,text,_=Prevline(25,file)
	if !strings.Contains(text,"import"){
		t.Fatal("get:"+text)
	}
	_,text,_=Nextline(25,file)
	if !strings.Contains(text,"os"){
		t.Fatal("get:"+text)
	}
}
