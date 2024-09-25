import sys
from Database import *
from Relation import *
from Tuple import *


def main():
    db = Database()
    db.initializeDatabase(sys.argv[1])

    ## Find the names of employees who work on all the projects controlled by
    ## department number 4.

    # (project[pno](
    #     (rename[essn](project[ssn](select[lname='Smith'](employee)))
    # join
    # works_on
    # )
    # )
    # union
    # project[pnumber](
    #     (rename[dnum](project[dnumber](select[lname='Smith'](
    #     (employee
    #     join
    #     rename[dname, dnumber, ssn, mgrstartdate](department)
    # )
    # )
    # )
    # )
    # join
    # projects
    # )
    # )
    # );

    employee = db.getRelation("EMPLOYEE")
    projects = db.getRelation("PROJECTS")
    workson = db.getRelation("WORKS_ON")
    departments = db.getRelation("DEPARTMENT")

    r1 = employee.select("col", "LNAME", "=", "str", "Smith")
    cols = ["SSN"]
    r2 = r1.project(cols)
    cols = ["ESSN"]
    r3 = r2.rename(cols)
    cols = ["PNO"]
    r4 = r3.join(workson)
    r5 = r4.project(cols)

    


    answer = r5
    print(answer)


main()