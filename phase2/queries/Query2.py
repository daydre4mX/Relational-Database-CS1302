import sys
from Database import *
from Relation import *
from Tuple import *

def main():
    db = Database()
    db.initializeDatabase(sys.argv[1])
    ## For every project located in "Stafford", list the project number, the 
    ## controlling department number, and the department manager's last name, address, 
    ## and birth date.

    ## project[pnumber,dnum,lname,address,bdate](
    ##   (
    ##    (select[plocation='Stafford'](projects) 
    ##     join 
    ##     rename[dname,dnum,ssn,mgrstartdate](department)
    ##    )
    ##    join employee
    ##   )
    ## )
    employee = db.getRelation("EMPLOYEE")
    projects = db.getRelation("PROJECTS")
    department = db.getRelation("DEPARTMENT")
    r1 = projects.select("col","PLOCATION","=","str","Stafford")
    cols = [ "DNAME", "DNUM", "SSN", "MGRSTARTDATE"]
    r2 = department.rename(cols)
    r3 = r1.join(r2)
    r4 = r3.join(employee)
    cols = [ "PNUMBER", "DNUM", "LNAME", "ADDRESS", "BDATE"]
    r5 = r4.project(cols)
    r5.setName("ANSWER")
    print(r5)

main()
