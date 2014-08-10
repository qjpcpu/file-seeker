package fu

import (
	"bufio"
	"os"
)

// 根据文件偏移量获取当前行
func Getline(cursor int64,file *os.File) (int64,string,error){
	_,err:=file.Seek(cursor,0)
	if err!=nil{
		return cursor,"",err
	}
	step,now,linelen:=int64(50),cursor,0
	//开始回溯文件指针
	for{
		if now-step<0{
			now=0
		}else{
			now=now-step
		}
		file.Seek(now,0)
		scanner := bufio.NewScanner(file)
		scanner.Scan()
		linelen=len(scanner.Bytes())
		if int64(linelen)+now >=cursor && now!=0{
			//继续回溯直到now+linelen<cursor
			continue
		}
		if now!=0{
			now+=int64(linelen)+1
		}
		for{
			scanner=bufio.NewScanner(file)
			file.Seek(now,0)
			scanner.Scan()
			linelen=len(scanner.Bytes())
			if now+int64(linelen)<cursor{
				now+=int64(linelen)+1
				continue
			}else{
				break
			}
		}
		break
	}
	data:=make([]byte,linelen)
	file.ReadAt(data,now)
	return now,string(data),nil
}

func Prevline(cursor int64,file *os.File) (int64,string,error){
	cursor,text,err:=Getline(cursor,file)
	if err!=nil{
		return cursor,text,err
	}
	if cursor==0{
		return cursor,text,nil
	}
	return Getline(cursor-1,file)
}

func Nextline(cursor int64,file *os.File) (int64,string,error){
	cursor,text,err:=Getline(cursor,file)
	if err!=nil{
		return cursor,text,err
	}
	next:=cursor+int64(len(text))+1
	fi,_:=file.Stat()
	if next>=fi.Size()-1{
		return cursor,text,nil
	}
	return Getline(next,file)
}
