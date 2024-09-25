import sys
from Database import *
from Relation import *
from Tuple import *

def main():
    db = Database()
    db.initializeDatabase(sys.argv[1])

    ## Retrieve the name and address of employees who work for the "Research" department.
    ##   project[fname,lname,address](
    ##       (rename[dname,dno,mgrssn,mgrstartdate](
    ##           select[dname='Research'](department)) 
    ##        join 
    ##        employee
    ##       )
    ## )
    department = db.getRelation("DEPARTMENT")
    employee = db.getRelation("EMPLOYEE")
    
    ## employee.join(
    ##       department.select("col","DNAME","=","str","Research")
    ##                 .rename(["DNAME","DNO","MGRSSN","MGRSTARTDATE"]))
    ##                 .project(["FNAME","LNAME","ADDRESS"])
    r1 = department.select("col","DNAME","=","str","Research")
    cols = [ "DNAME", "DNO", "MGRSSN", "MGRSTARTDATE"]
    r2 = r1.rename(cols)
    r3 = r2.join(employee)
    cols = [ "FNAME", "LNAME", "ADDRESS"]
    r4 = r3.project(cols)
    r4.setName("ANSWER")
    print(r4)

main()
