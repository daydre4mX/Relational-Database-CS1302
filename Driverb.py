from Database import *
from Relation import *
from Tuple import *

def main():
    db = Database()
    attr1 = ["SID","SNAME","PHONE","MAJOR","GPA"]
    dom1 = ["INTEGER","VARCHAR","INTEGER","VARCHAR","DECIMAL"]
    r1 = Relation("STUDENT",attr1,dom1)
    t = Tuple(attr1,dom1)
    t.addComponent(1111)
    t.addComponent("Robert Adams")
    t.addComponent(1234)
    t.addComponent("Computer Science")
    t.addComponent(4.0)
    r1.addTuple(t)
    t = Tuple(attr1,dom1)
    t.addComponent(1112)
    t.addComponent("Charles Bailey")
    t.addComponent(5656)
    t.addComponent("Computer Science")
    t.addComponent(3.5)
    r1.addTuple(t)
    t = Tuple(attr1,dom1)
    t.addComponent(1113)
    t.addComponent("David Beatle")
    t.addComponent(1212)
    t.addComponent("Mathematics")
    t.addComponent(3.5)
    r1.addTuple(t)
    t = Tuple(attr1,dom1)
    t.addComponent(1114)
    t.addComponent("Graham Gooch")
    t.addComponent(5678)
    t.addComponent("Computer Science")
    t.addComponent(3.5)
    r1.addTuple(t)

    db.addRelation(r1)

    attr2 = ["SID","COURSE","GRADE"]
    dom2 = ["INTEGER","VARCHAR","VARCHAR"]
    r2 = Relation("ENROLL",attr2,dom2)
    t = Tuple(attr2,dom2)
    t.addComponent(1111)
    t.addComponent("Csc 4710")
    t.addComponent("A")
    r2.addTuple(t)
    t = Tuple(attr2,dom2)
    t.addComponent(1114)
    t.addComponent("Csc 2310")
    t.addComponent("B")
    r2.addTuple(t)
    t = Tuple(attr2,dom2)
    t.addComponent(1114)
    t.addComponent("Csc 2310")
    t.addComponent("A")
    r2.addTuple(t)

    db.addRelation(r2)

    attr3 = ["COURSE","TITLE","CREDITS"]
    dom3 = ["VARCHAR","VARCHAR","INTEGER"]
    r3 = Relation("COURSES",attr3,dom3)
    t = Tuple(attr3,dom3)
    t.addComponent("Csc 4710")
    t.addComponent("Database Systems")
    t.addComponent(4)
    r3.addTuple(t)
    t = Tuple(attr3,dom3)
    t.addComponent("Csc 2010")
    t.addComponent("Java I")
    t.addComponent(3)
    r3.addTuple(t)
    t = Tuple(attr3,dom3)
    t.addComponent("CSc 2310")
    t.addComponent("Java II")
    t.addComponent(3)
    r3.addTuple(t)

    db.addRelation(r3)

    print(r1)
    print(r2)
    print(r3)

    # Lets formulate a query to list names of students who got "A" in course 
    # titled "Database Systems"
    t1 = r2.select("col","GRADE","=","str","A")
    t2 = r3.select("col","TITLE","=","str","Database Systems")
    t3 = r1.join(t1)
    t4 = t3.join(t2)
    attr = ["SNAME"]
    result = t4.project(attr)
    result.setName("ANSWER")
                              
    print(result)

main()
