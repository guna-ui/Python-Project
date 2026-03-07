import csv


class CSVReader:
    def __init__(self,filename):
        self.filename=filename
        self.headers=[]
        self.records=[]
    def read_csv(self):
        try:
            with open(self.filename,mode='r') as f:
                reader=csv.reader(f)
                header=next(reader)
                self.headers.append(header)
                for row in reader:
                    self.records.append(row)
            return header,self.records
        except FileNotFoundError as e:
            print(f"file not found :{e}")
            return [],[]

class DataCleaner:
    def __init__(self,header,records):
        self.header=header
        self.records=records
        self.originaldata=len(records)
    def read_data(self):
        seen=set()
        data=[],
        clean_data=[]
        duplicate=[]
        for row in self.records:
            row_id=row[0]
            # seen.add(row_id)
            if('' not in row and row[2].isdigit() and int(row[2])>0):
                if(row_id not in seen):
                    clean_data.append(row)
                    seen.add(row_id)
                else:
                    duplicate.append(row_id)
        return f"removed records:{len(self.records)-len(clean_data)}"
                
        
        
        

reader=CSVReader('students.csv')
header,records=reader.read_csv()
x=DataCleaner(header=header,records=records)
print(x.read_data())


