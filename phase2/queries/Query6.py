import sys
from Database import *
from Relation import *
from Tuple import *

def main():
    db = Database()
    db.initializeDatabase(sys.argv[1])
    ## Retrieve the names of employees who have no dependents.
    ##
    ## project[lname,fname](
    ##  ( ( project[ssn](employee) 
    ##       minus project[essn](dependent)
    ##    ) 
    ##    join 
    ##    employee
    ##  )
    ## )
    
    employee = db.getRelation("EMPLOYEE")
    dependent = db.getRelation("DEPENDENT")
    cols = ["SSN"]
    r1 = employee.project(cols)
    cols = ["ESSN"]
    r2 = dependent.project(cols)
    r3 = r1.minus(r2)
    r4 = r3.join(employee)
    cols = ["LNAME", "FNAME"]
    r5 = r4.project(cols)
    r5.setName("ANSWER")
    print(r5)

main()
