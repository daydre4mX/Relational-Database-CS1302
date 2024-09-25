import sys
from Database import *
from Relation import *
from Tuple import *

def main():
    db = Database()
    db.initializeDatabase(sys.argv[1])

    ## Find the names of employees who work on all the projects controlled by 
    ## department number 4.

    ## project[lname,fname](
    ##  (employee
    ##   join
    ##   (project[ssn](employee)
    ##    minus
    ##    project[ssn](
    ##     (
    ##       (project[ssn](employee) 
    ##        times  
    ##        project[pnumber](select[dnum=4](projects))
    ##       )
    ##       minus
    ##       rename[ssn,pnumber](project[essn,pno](works_on))
    ##     )
    ##    )
    ##   )
    ##  )
    ## )
    
    employee = db.getRelation("EMPLOYEE")
    projects = db.getRelation("PROJECTS")
    workson = db.getRelation("WORKS_ON")
    
    r1 = projects.select("col","DNUM","=","num",4).project(["PNUMBER"])

    r2 = employee.project(["SSN"]).times(r1)
    r3 = workson.project(["ESSN","PNO"]).rename(["SSN","PNUMBER"])
    r4 = r2.minus(r3).project(["SSN"])
    r5 = employee.project(["SSN"]).minus(r4)
    answer = employee.join(r5).project(["LNAME","FNAME"])
    answer.setName("ANSWER")
   
    print(answer)

main()
