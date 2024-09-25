from Database import *
from Relation import *
import sys

def main():
    db = Database()
    db.initializeDatabase(sys.argv[1])
    db.displaySchema()
    r1 = db.getRelation("BAR")
    print(r1)
    r2 = db.getRelation("DRINKER")
    print(r2)
    r3 = db.getRelation("SELLS")
    print(r3)

main()