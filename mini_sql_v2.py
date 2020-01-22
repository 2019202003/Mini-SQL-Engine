import sqlparse
import itertools
import sys
def max(A):
    global result_rows
    maxi=-99999
    for i in result_rows:
        if(i[A]>maxi):
            maxi=i[A]
    return maxi

def sum(A):
    global result_rows
    sumi=0
    for i in result_rows:
        sumi+=i[A]
   
    return sumi

def avg(A):
    global result_rows
   
    return float(sum(A))/len(result_rows)

def min(A):
    global result_rows
    mini=999999
    for i in result_rows:
        if(i[A]<mini):
            mini=i[A]
    return mini


   



def reading_meta(lines):
        
    jj={}
    pp=0
    cd=0
    kk=lines
    for i in kk:
        i=i.rstrip('\r\n')
        if(pp==0):
            table_name=i
            pp=1
            jj[table_name]=[]
            continue   
        elif('<end_table>' in i):
            pp=0     
        elif(pp):
            jj[table_name].append(i)
    return jj



def eliminate_db(database):
    for i in database:
        for j in range(len(database[i])):
            database[i][j]=database[i][j].rstrip('\r\n')
           # print(database[i][j])
            database[i][j]=database[i][j].split(',')
            #print(database[i][j])
            for k in range(len(database[i][j])):
                database[i][j][k]=int(database[i][j][k])
                #print(database[i][j][k])
   
    return database
def generate_rrequest_tree(rrequest):
    rrequest_tree={}
    rrequest_tree['where']="-"
    rrequest_tree['from']="-"
    rrequest_tree['select']="-"
   
   
    try:
        #temp1=rrequest.split(' where ')[0]
        temp=rrequest.split(' where ')[1]
        #print(temp)
        #print('sankha')
        rrequest_tree['where']=" "+temp
        #print('sankha')
        #print(rrequest_tree)
        rrequest=rrequest.split(' where ')

        rrequest=rrequest[:-1]
        #rrequest1=rrequest[-1:]
        

       # print(rrequest)
        rrequest=str(rrequest[0])
        #print(rrequest)
    except:
        pass
   
    try:
        temp=rrequest.split(' from ')[1]
        #print(temp)
        rrequest_tree['from']=temp.split(',')
        
        rrequest=rrequest.split(' from ')
        rrequest=rrequest[:-1]
        rrequest=str(rrequest[0])
    except:
        pass

    try:
        temp=rrequest.split('select ')[1]
        #print(temp)
        rrequest_tree['select']=temp.split(',')
        #print(rrequest_tree['select'])
        rrequest=rrequest.split('select ')
        rrequest=rrequest[:]
        #print(rrequest)
        rrequest=str(rrequest[0])
        #print(rrequest)
    except:
        pass
   
   
   
   
    return rrequest_tree

def preprocess_rrequest(rrequest):
    rrequest=sqlparse.format(rrequest,keyword_case='lower')
    #print(rrequest)
    rrequest=str(rrequest)
    #print(rrequest)
    rrequest=rrequest.replace(' , ',',')
    rrequest=rrequest.replace(', ',',')
    rrequest=rrequest.replace(' ,',',')

    rrequest=rrequest.replace(' = ','==')
    rrequest=rrequest.replace('= ','==')
    rrequest=rrequest.replace(' =','==')
   
   
   

    rrequest=rrequest.replace(' <= ','<=')
    rrequest=rrequest.replace('<= ','<=')
    rrequest=rrequest.replace(' <=','<=')

    rrequest=rrequest.replace(' >= ','>=')
    rrequest=rrequest.replace('>= ','>=')
    rrequest=rrequest.replace(' >=','>=')

    rrequest=rrequest.replace(' < ','<')
    rrequest=rrequest.replace('< ','<')
    rrequest=rrequest.replace(' <','<')

    rrequest=rrequest.replace(' > ','>')
    rrequest=rrequest.replace('> ','>')
    rrequest=rrequest.replace(' >','>')







    return rrequest




if __name__ == '__main__':
    try:
        f=open('metadata.txt','r')
        lines=f.readlines()
        for i in lines:
            if('<begin_table>' in i):
                lines.remove(i)
        metadata=reading_meta(lines)
        #print(metadata)
        database={}
        for i in metadata:
            ff=open(i+'.csv','r')
            data=ff.readlines()
            database[i]=data
        #print(database)
        database=eliminate_db(database)
        #print(database)
        #print(database)
        distinct=0
        #rrequest="Select * from table1,table2 where table1.B=table2.B and table2.D>20"#where a=1 AND p=2;";
        rrequest=sys.argv[1]
        #print(rrequest)
        rrequest=rrequest.strip(' ')
        #print(rrequest)
        print("start. .  ..")
        if(rrequest[-1]!=';'):
            print("huhuhu lulu")
            sys.exit(0)
       
        rrequest=rrequest.strip(';')
        rrequest=rrequest.strip(' ')
       
        rrequest=preprocess_rrequest(rrequest)
       
        if('select distinct ' in rrequest):
            distinct=1
            rrequest=rrequest.replace('select distinct','select')
            #print(rrequest)
       
        rrequest_tree=generate_rrequest_tree(rrequest)
        #print(rrequest_tree)
       
        tables_data=[]
        for i in rrequest_tree['from']:
            #print('1')
            tables_data.append(database[i])
        #print('table data')
        #print(tables_data)
           
        column_headers_type1=[]
        column_headers_type2=[]
       
        for i in rrequest_tree['from']:
            for j in metadata[i]:
                column_headers_type1.append(j)
                column_headers_type2.append(i+'.'+j)
               
        #print(column_headers_type1)
       # print(column_headers_type2)
           
        result_rows=[]  
        for i in itertools.product(*tables_data):
            #print(i)
            li=[]
            for j in i:
                li=li+j
                #print(j)
            result_rows.append(li)
        #print(result_rows)
        for i in column_headers_type1:
               if("("+i+")" in rrequest_tree['where']):
                          rrequest_tree['where']=rrequest_tree['where'].replace("("+i+")","("+str(column_headers_type1.index(i))+")")
               if(" "+i+"=" in rrequest_tree['where']):
                          rrequest_tree['where']=rrequest_tree['where'].replace(" "+i+"="," x["+str(column_headers_type1.index(i))+"]==")
                          #print(rrequest_tree['where'])
                          #print('jj')
               if(" "+i+"<=" in rrequest_tree['where']):
                          rrequest_tree['where']=rrequest_tree['where'].replace(" "+i+"<="," x["+str(column_headers_type1.index(i))+"]<=")
               if(" "+i+">=" in rrequest_tree['where']):
                          rrequest_tree['where']=rrequest_tree['where'].replace(" "+i+">="," x["+str(column_headers_type1.index(i))+"]>=")
               if(" "+i+"<" in rrequest_tree['where']):
                          rrequest_tree['where']=rrequest_tree['where'].replace(" "+i+"<"," x["+str(column_headers_type1.index(i))+"]<")
               if(" "+i+">" in rrequest_tree['where']):
                          rrequest_tree['where']=rrequest_tree['where'].replace(" "+i+">"," x["+str(column_headers_type1.index(i))+"]>")
        #print(rrequest_tree)
       
        for i in column_headers_type2:
            if("("+i+")" in rrequest_tree['where']):
                          rrequest_tree['where']=rrequest_tree['where'].replace("("+i+")","("+str(column_headers_type2.index(i))+")")
            if(" "+i+"=" in rrequest_tree['where']):
                rrequest_tree['where']=rrequest_tree['where'].replace(" "+i+"="," x["+str(column_headers_type2.index(i))+"]==")
           
            if(" "+i+"<=" in rrequest_tree['where']):
                rrequest_tree['where']=rrequest_tree['where'].replace(" "+i+"<="," x["+str(column_headers_type2.index(i))+"]<=")
            if(" "+i+">=" in rrequest_tree['where']):
                rrequest_tree['where']=rrequest_tree['where'].replace(" "+i+">="," x["+str(column_headers_type2.index(i))+"]>=")
            if(" "+i+"<" in rrequest_tree['where']):
                rrequest_tree['where']=rrequest_tree['where'].replace(" "+i+"<"," x["+str(column_headers_type2.index(i))+"]<")
            if(" "+i+">" in rrequest_tree['where']):
                rrequest_tree['where']=rrequest_tree['where'].replace(" "+i+">"," x["+str(column_headers_type2.index(i))+"]>")
       
        rrequest_type=1
        #print(rrequest_tree) 
        #print(rrequest.split('where')[1])
        try:
            if ("select max(" in rrequest or "select min(" in rrequest or "select sum(" in rrequest or "select avg(" in rrequest):
                rrequest_type=2
            elif('.' in rrequest.split('where')[1].split('<')[0] and '.' in rrequest.split('where')[1].split('<')[1]):
                #print('hg')
                rrequest_type=3
            
            elif(('.' in rrequest.split('where')[1].split('=')[0] and '.' in rrequest.split('where')[1].split('=')[1]) or ('.' in rrequest.split('where')[1].split('>')[0] and '.' in rrequest.split('where')[1].split('>')[1]) or ('.' in rrequest.split('where')[1].split('>=')[0] and '.' in rrequest.split('where')[1].split('>=')[1]) or ('.' in rrequest.split('where')[1].split('<')[0] and '.' in rrequest.split('where')[1].split('<')[1]) or ('.' in rrequest.split('where')[1].split('<=')[0] and '.' in rrequest.split('where')[1].split('<=')[1])):
                rrequest_type=3
        except:
            pass

        try:
            if ("select max(" in rrequest or "select min(" in rrequest or "select sum(" in rrequest or "select avg(" in rrequest):
                rrequest_type=2
            elif('.' in rrequest.split('where')[1].split('>')[0] and '.' in rrequest.split('where')[1].split('>')[1]):
                #print('oo')
                rrequest_type=3
        except:
            pass

        try:
            if ("select max(" in rrequest or "select min(" in rrequest or "select sum(" in rrequest or "select avg(" in rrequest):
                rrequest_type=2
            elif('.' in rrequest.split('where')[1].split('=')[0] and '.' in rrequest.split('where')[1].split('=')[1]):
                #print('oo')
                rrequest_type=3
        except:
            pass

           
                 
        print(rrequest_type)
        if(rrequest_tree['where']!='-' and rrequest_type!=3):
            #print('hhh')
            result_rows=filter(lambda x: eval(rrequest_tree['where']), result_rows)
       
        #print(result_rows)
       
        if(rrequest_type==1):
            #print "Type 1 rrequest"  
            result_columns=[]
            for i in rrequest_tree['select']:
                if('*' in i):
                    for j in range(len(column_headers_type1)):
                        result_columns.append(j)
                    break
                if('.' in i):
                    #print(column_headers_type2.count(i))
                    if(column_headers_type2.count(i)==0 ):
                        #print('2')
                        #print "error"
                        break
                    result_columns.append(column_headers_type2.index(i))
                    #print('22')
                else:
                    if(column_headers_type1.count(i)==0 ): #if more than one column with same name or no column
                        #print('OO'+i)
                        #print "error"
                        break
                    result_columns.append(column_headers_type1.index(i))
                    #print('33')
           # print(result_columns)
            str_row=""
            for j in result_columns:
                str_row=str_row+column_headers_type2[j]+","
            print(str_row.strip(','))
            #print('6')
           
            final_answer=[]
            for i in result_rows:
                #print(i)
                str_row=""
                for j in result_columns:
                    str_row=str_row+str(i[j])+','
                final_answer.append(str_row.rstrip(','))
               
            if(distinct==1):
                final_answer=set(final_answer)
                final_answer=list(final_answer)
           
            for i in final_answer:
                print(i)        
        if(rrequest_type==2):
            cols=[]
            for k in range(len(rrequest_tree['select'])):
                for i in column_headers_type1:
                   
                    if("("+i+")" in rrequest_tree['select'][k]):
                        cols.append(rrequest_tree['select'][k].replace(i,column_headers_type2[column_headers_type1.index(i)]))          
                        rrequest_tree['select'][k]=rrequest_tree['select'][k].replace("("+i+")","("+str(column_headers_type1.index(i))+")")
                     
                for i in column_headers_type2:
                    if("("+i+")" in rrequest_tree['select'][k]):
                                  cols.append(rrequest_tree['select'][k])
                                  rrequest_tree['select'][k]=rrequest_tree['select'][k].replace("("+i+")","("+str(column_headers_type2.index(i))+")")
                   
            #print(cols)  
            #print "Type 2 rrequest"  
            results=[]
            
            for i in rrequest_tree['select']:
                #print(i);
                results.append(eval(i))
            #print(results)
           
            str_row=""
            for j in cols:
                str_row=str_row+str(j)+','
            print(str_row.rstrip(','))
           
            str_row=""
            for j in results:
                str_row=str_row+str(j)+','
            print(str_row.rstrip(','))
        if(rrequest_type==3):
            #print('type 3')
            temp=rrequest.split('where')[1]
            temp=temp.rstrip(';')
            temp=temp.strip(' ')
            print(temp)
           
            for i in column_headers_type2:
                    if(i in temp):
                        temp=temp.replace(i,"x["+str(column_headers_type2.index(i))+"]")
                        prev=i
            print(temp)
            if('=' in temp and '>=' not in temp and '<=' not in temp):
                #print('yes')
                temp=temp.replace('=','==')
            
            
            print(temp)
            result_rows=filter(lambda x: eval(temp), result_rows)
            print(result_rows)
            #print "Type 3 rrequest"  
            result_columns=[]
            for i in rrequest_tree['select']:
                if('*' in i):
                    #print("hi")
                    for j in range(len(column_headers_type1)):
                        result_columns.append(j)
                    break
                if('.' in i):
                    if(column_headers_type2.count(i)==0 ):
                        #print "error"
                        break
                    result_columns.append(column_headers_type2.index(i))
                else:
                    if(column_headers_type1.count(i)==0 ):
                        #print "error"
                        break
                    result_columns.append(column_headers_type1.index(i))
            print(result_columns)
            #print('line 427')
            result_columns.remove(column_headers_type2.index(prev))
            str_row=""
            for j in result_columns:
                #print('432')
                str_row=str_row+column_headers_type2[j]+","
            print(str_row.strip(','))
            #print('hh')
            final_answer=[]
            for i in result_rows:
                #print(i)
                str_row=""
                for j in result_columns:
                    str_row=str_row+str(i[j])+','
                    #print(str)
                final_answer.append(str_row.rstrip(','))
               
            if(distinct==1):
                final_answer=set(final_answer)
                final_answer=list(final_answer)
           
            for i in final_answer:
                print(i)
    except:
        print("An Error Occured. Aborting !")