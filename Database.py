from Relation import *
from Tuple import *


class Database:

    def __init__(self):
        self.relations = {}

    # Create the database object by reading data from several files in directory dir
    def initializeDatabase(self, dir):
        database = open(f'{dir}/catalog.dat', 'r')
        numberRelations = database.readline().strip('\n')
        for i in range(int(numberRelations)):
            relationName = database.readline().strip('\n')
            attributeNumber = int(database.readline().strip('\n'))
            attributes = []
            domains = []

            for j in range(attributeNumber):
                attributeName = database.readline().strip('\n')
                attributeDomain = database.readline().strip('\n')

                attributes.append(attributeName)
                domains.append(attributeDomain)

            relation = Relation(relationName, attributes, domains)

            # get data for tuples
            fileName = f'{dir}/{relationName}.dat'

            relationFile = open(fileName, 'r')
            numberTuples = int(relationFile.readline().strip('\n'))

            lineCount = len(relationFile.readlines())

            relationFile.seek(len(str(numberTuples)) + 1)

            for k in range(numberTuples + 1):
                tupleList = Tuple(attributes, domains)

                for l in range(lineCount // numberTuples):
                    e = relationFile.readline().strip('\n')
                    tupleList.addComponent(e)

                relation.addTuple(tupleList)
            relationFile.close()

            relation.table.pop()
            self.addRelation(relation)
        database.close()

    # Add relation r to Dictionary if relation does not already exists.
    # return True on successful add; False otherwise
    def addRelation(self, r):
        if r.name not in self.relations.keys():
            self.relations[r.name] = r
            return True
        return False

    # Delete relation with name rname from Dictionary if relation exists.
    # return True on successful delete; False otherwise
    def deleteRelation(self, rname):
        if rname in self.relations.keys():
            del self.relations[rname]
            return True
        return False

    # Retrieve and return relation with name rname from Dictionary.
    # return None if it does not exist.
    def getRelation(self, rname):
        if rname in self.relations.keys():
            return self.relations[rname]
        return None

    # Return database schema as a String
    def displaySchema(self):
        relationSchema = ''

        for key in self.relations:
            relationSchema += self.relations[key].displaySchema() + '\n'

        return '\n' + relationSchema
