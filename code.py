import sys
import math
import heapq
import threading
from threading import Thread
from threading import *
import time
import os
def mergechunks(totalchunks,sortcolm,outputfile):
	f=open(outputfile,"w")
	fileptrs=[0]
	heap=[]
	dict1={}	
	for i in range(1,totalchunks+1):
		f1=open("chunk"+str(i)+".txt","r")
		key=""
		temp=f1.readline()
		if(temp==""):
			continue	
		dict1[i]=temp
		temp=temp.strip("\n").split(" ")
		#print(temp)
		if(temp[-1]==""):
			temp=temp[:-1]
		#print(temp)
		list2=[]
		#print(sortcolm,totalcols)
		#print(temp)		
		for j in sortcolm:
			key+=temp[j]
		#print(key)
		heap.append((key,i))
		fileptrs.append(f1)
	heapq.heapify(heap)
	while(len(heap)>0):
		#print(heap)
		#print(dict1)
		temp=()
		temp=heapq.heappop(heap)
		key,ind=temp[0],temp[1]
		#print(ind,len(fileptrs))
		f2=fileptrs[ind]
		next=f2.readline()
		fileptrs[ind]=f2
		#print(dict1[ind])
		f.write(dict1[ind])		
		dict1[ind]=next
		next=next.strip("\n").split(" ")
		list2=[]
		if(next[-1]==""):
			next=next[:-1]
		if(len(next)>0):
			key=""	
			for j in sortcolm:
				key+=next[j]
			#print(key)
			heapq.heappush(heap,(key,ind))
	f.close()
	for i in range(1,totalchunks+1):
		os.remove("chunk"+str(i)+".txt")
def sortchunks(i,sortcolm):	
	#print("Sorting #"+str(i)+" subfile")
	f=open("chunk"+str(i)+".txt","r")
	temp=[]
	cur=f.readline().strip("\n").split(" ")
	list2=[]
	if(cur[-1]==""):
		cur=cur[:-1]
	while(len(cur)>0):
		#print(cur)
		temp.append(cur)
		#print(temp)
		cur=f.readline().strip("\n").split(" ")
		list2=[]
		if(cur[-1]==""):
			cur=cur[:-1]
	f.close()
	#print(temp)		
	for j in sortcolm:
		#print("in loop")
		#print(temp[:100])
		temp.sort(key=lambda x:x[j])
	#print(temp[:100])
	#print("Writing to disk #"+str(i))
	#print("--------------------------------------------------")				
	f=open("chunk"+str(i)+".txt","w")
	for j in range(len(temp)):
		str1=""
		list2=[]
		for k in range(2):
			str1+=temp[j][k]+" "
		str1+="\n"
		f.write(str1)
	f.close()
def createchunks(inputfile,tuplesinchunk):
	f=open(inputfile,"r")
	cur=f.readline()
	chunkno=1
	while(cur!=""):
		f1=open("chunk"+str(chunkno)+".txt","w")
		i=0
		while(cur!="" and i<tuplesinchunk):
			f1.write(cur)
			cur=f.readline()
			i+=1
		if(cur==""):
			break
		f1.close()
		chunkno+=1
	f.close()
	return chunkno
def divide(filename,tuplesinchunk):
	cur=0
	temp="$"
	fnum=1
	f1=open(filename,"r")
	f=open(filename+str(fnum),"w")
	flg=1
	while(1):
		temp=f1.readline()
		if(temp==""):
			f.close()
			f1.close()
			break
		f.write(temp)
		flg=0
		cur+=1
		if(tuplesinchunk==cur):
			flg=1
			cur=0
			fnum+=1
			f.close()
			f=open(filename+str(fnum),"w")
	if(flg==1):
		os.remove(filename+str(fnum))		
	os.remove(filename)
def dividehash(filename,tuplesinchunk):
	cur=0
	temp="$"
	fnum=1
	f1=open(filename,"r")
	f=open(filename+str(fnum),"w")
	flg=1
	while(1):
		temp=f1.readline()
		if(temp==""):
			f.close()
			f1.close()
			break
		f.write(temp)
		flg=0
		cur+=1
		if(tuplesinchunk==cur):
			flg=1
			cur=0
			fnum+=1
			f.close()
			f=open(filename+str(fnum),"w")
	if(flg==1):
		os.remove(filename+str(fnum))
		return fnum-1
	return fnum		
def softmergejoin(chunksofR,chunksofS,total,filename1,filename2):
	curR=1
	curS=1
	ptrR=0
	ptrS=0
	f=open("R_S_join.txt","w")
	mark=[1,0]
	while(curR<=chunksofR and curS<=chunksofS):
		tempR=curR
		tempS=curS
		listR=[]
		listS=[]
		f1=open("R"+str(curR),"r")
		data=""
		while(1):
			data=f1.readline()
			if(data==""):
				break
			data=data.strip("\n").split(" ")
			listR.append(data)
		f1.close()
		f1=open("S"+str(curS),"r")
		data=""		
		while(1):
			data=f1.readline()
			if(data==""):
				break
			data=data.strip("\n").split(" ")
			listS.append(data)
		f1.close()		
		while(ptrR<len(listR) and ptrS<len(listS)):
			#print(mark,ptrR,ptrS)
			if(mark==[1,0]):
				if(listS[0][0]!=listR[ptrR][1]):							
					
					while(ptrR<len(listR) and ptrS<len(listS) and listR[ptrR][1]<listS[ptrS][0]):
						ptrR+=1
					if(ptrR==len(listR) or ptrS==len(listS)):
						break
					while(ptrR<len(listR) and ptrS<len(listS) and listR[ptrR][1]>listS[ptrS][0]):
						ptrS+=1
					if(ptrR==len(listR) or ptrS==len(listS)):
						break
					
					mark=[curS,ptrS]
			#print(mark,ptrR,ptrS)	
			if(listR[ptrR][1]==listS[ptrS][0]):
				f.write(listR[ptrR][0]+" "+listR[ptrR][1]+" "+listS[ptrS][1]+"\n")
				ptrS+=1
			else:
				ptrS=mark[1]
				curS=mark[0]
				ptrR+=1
				mark=[1,0]
				listS=[]
				f1=open("S"+str(curS),"r")
				data=""		
				while(1):
					data=f1.readline()
					if(data==""):
						break
					data=data.strip("\n").split(" ")
					listS.append(data)
				f1.close()
			
		if(ptrS==len(listS)):
			curS+=1
			ptrS=0
		if(curS==chunksofS+1):
			ptrS=mark[1]
			curS=mark[0]
			ptrR+=1
			mark=[1,0]
			listS=[]
			f1=open("S"+str(curS),"r")
			data=""		
			while(1):
				data=f1.readline()
				if(data==""):
					break
				data=data.strip("\n").split(" ")
				listS.append(data)
			f1.close()
		if(ptrR==len(listR)):
			curR+=1
			ptrR=0
	for i in range(1,chunksofS+1):
		os.remove("S"+str(i))
	for i in range(1,chunksofR+1):
		os.remove("R"+str(i))
def hashjoin(chunksofR,chunksofS,filename1,filename2,m):
	f2=open("R_S_join.txt","w")
	for i in range(1,chunksofS+1):
		f=open(filename2+str(i),"r")
		cur=f.readline()
		while(cur!=""):
			cur=cur.strip("\n").split(" ")
			val=0
			for j in range(len(cur[0])):
				val+=ord(cur[0][j])*(17**j)
			val=val%m
			f1=open("S"+str(val),"a+")
			f1.write(cur[0]+" "+cur[1]+"\n")
			f1.close()
			cur=f.readline()
		f.close()
	for i in range(1,chunksofR+1):
		f=open(filename1+str(i),"r")
		cur=f.readline()
		while(cur!=""):
			cur=cur.strip("\n").split(" ")
			val=0
			for j in range(len(cur[1])):
				val+=ord(cur[1][j])*(17**j)
			val=val%m
			try:
				f1=open("S"+str(val),"r")
			except:
				cur=f.readline()
				continue
			temp=f1.readline()		
			while(temp!=""):
				temp=temp.strip("\n").split(" ")
				if(temp[0]==cur[1]):
					f2.write(cur[0]+" "+cur[1]+" "+temp[1]+"\n")
				temp=f1.readline()
			cur=f.readline()
		f.close()
	f2.close()
	for i in range(1,chunksofS+1):
		os.remove(filename2+str(i))
	for i in range(1,chunksofR+1):
		os.remove(filename1+str(i))
def cls(m):
	for i in range(m):
		try:	
			os.remove("S"+str(i))
		except:
			continue
def main():
	starttime=time.time()
	filename1=sys.argv[1]
	filename2=sys.argv[2]
	op=sys.argv[3]
	m=int(sys.argv[4])
	if(op=="sort"):
	#m=float(input())
		chunksofR=createchunks(filename1,m*100)		
		for i in range(chunksofR):
			sortchunks(i+1,[1])
		mergechunks(chunksofR,[1],"R")
		divide("R",m*100)
		chunksofS=createchunks(filename2,m*100)	
		for i in range(chunksofS):
			sortchunks(i+1,[0])			
		mergechunks(chunksofS,[0],"S")
		divide("S",m*100)
		softmergejoin(chunksofR,chunksofS,m*100,filename1,filename2)
	elif(op=="hash"):
		chunksofR=dividehash(filename1,m*100)
		chunksofS=dividehash(filename2,m*100)
		hashjoin(chunksofR,chunksofS,filename1,filename2,m)			
		cls(m)	
	endtime=time.time()		
	print(endtime-starttime)
main()
