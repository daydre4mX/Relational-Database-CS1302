from Tuple import Tuple


class Relation:

    def __init__(self, name, attributes, domains):
        self.name = name.upper()  # name of relation
        self.attributes = [a.upper() for a in attributes]  # list of names of attributes
        self.domains = [d.upper() for d in domains]  # list of "INTEGER", "DECIMAL", "VARCHAR"
        self.table = []  # list of tuple objects

    # Returns True if attribute with name aname exists in relation schema;
    # False otherwise
    def attribute_exists(self, aname):
        if aname in self.attributes:
            return True

        return False

    # Returns attribute type of attribute aname; return None if not present
    def attribute_type(self, aname):
        if self.attribute_exists(aname):
            return self.domains[self.attributes.index(aname)]

        return None

    # Return relation schema as String
    def displaySchema(self):
        combinedAttrDom = ''
        for i in range(len(self.attributes)):
            combinedAttrDom += self.attributes[i] + ':' + self.domains[i] + ','

        return self.name + '(' + combinedAttrDom[:-1] + ')'

    # Set name of relation to rname
    def setName(self, rname):
        self.name = rname.upper()

    # Add tuple tup to relation; Duplicates are fine.
    def addTuple(self, tup):
        self.table.append(tup)

    # Remove duplicate tuples from this relation
    def removeDuplicates(self):
        checkAgainstTupleList = [
            0]  # has a dummy value so the loop will actually run, yes, i know this is dirty, but it works.

        for i in range(len(self.table)):
            duplicateTrue = False

            for j in range(len(checkAgainstTupleList)):
                if self.table[i].equals(checkAgainstTupleList[j]):
                    duplicateTrue = True
            if not duplicateTrue:
                checkAgainstTupleList.append(self.table[i])

        checkAgainstTupleList.pop(0)
        self.table = checkAgainstTupleList

    # checks if given tuple is in this relation's table.
    def member(self, t):
        for tuple in self.table:
            if tuple.equals(t):
                return True
        return False

    # adds all tuples from r2 into this relation without duplicates.
    def union(self, r2):
        attrs = self.attributes.copy()
        doms = self.domains.copy()

        cloneRel = Relation("ANSWER", attrs, doms)

        for t in self.table:
            cloneRel.addTuple(t.clone(attrs))

        for t in r2.table:
            cloneRel.addTuple(t.clone(attrs))

        cloneRel.removeDuplicates()

        return cloneRel

    def intersect(self, r2):
        attrs = self.attributes.copy()
        doms = self.domains.copy()

        cloneRel = Relation("ANSWER", attrs, doms)

        for t in self.table:
            if r2.member(t):
                cloneRel.addTuple(t.clone(attrs))

        return cloneRel

    def minus(self, r2):
        attrs = self.attributes.copy()
        doms = self.domains.copy()

        cloneRel = Relation("ANSWER", attrs, doms)

        for t in self.table:
            if not r2.member(t):
                cloneRel.addTuple(t.clone(attrs))

        return cloneRel

    ## The rename method takes as parameter an array list of new column names, cnames,
    ## and returns a new relation object that contains the same set of tuples, but with
    ## new columns names. We can assume that the size of cnames is same as size of this.attributes
    def rename(self, cnames):
        newAttr = []
        newDoms = []

        newAttr = cnames.copy()
        newDoms = self.domains.copy()

        newRel = Relation('ANSWER', newAttr, newDoms)

        for tuple in self.table:
            newRel.addTuple(tuple.clone(newAttr))

        return newRel

    ## The times method returns the cartesian product of two relations.
    ## As an example, let R and S be the following two relations:
    ## R(A:VARCHAR, B:INTEGER, C:INTEGER) and S(B:INTEGER, C:INTEGER, D:DECIMAL)
    ## and let R contain the tuples {<jones",20,200>, <smith",30,300> and
    ## let S contian the tuples {<1,2,2.5>, <100,200,3.86>}
    ## The R times S would have the schema
    ## R_TIMES_S(A:VARCHAR, R.B:INTEGER, R.C:INTEGER, S.B:INTEGER, S.C:INTEGER, D:DECIMAL)
    ## and the tuples: {<jones",20,200,1,2,2.5>, <jones",20,200,100,200,3.86>,
    ##                  <smith",30,300,1,2,2.5>, <smith",30,300,100,200,3.86>}
    ## Notice the tuples in the output are formed by combining tuples in the
    ## input relations in all possible ways, maintaining the order of columns
    def times(self, r2):
        newAttr = []
        newDoms = []

        for attr in self.attributes:
            if r2.attribute_exists(attr):
                newAttr.append(self.name + "." + attr)
            else:
                newAttr.append(attr)
            newDoms.append(self.attribute_type(attr))

        for attr in r2.attributes:
            if self.attribute_exists(attr):
                newAttr.append(r2.name + "." + attr)
            else:
                newAttr.append(attr)
            newDoms.append(r2.attribute_type(attr))

        newRel = Relation("ANSWER", newAttr, newDoms)

        tupTable = []
        for tuple in self.table:
            for secondTuple in r2.table:
                tupTable.append(tuple.concatenate(secondTuple, newAttr, newDoms))

        newRel.table = tupTable.copy()

        return newRel

    ## This methods takes as input a list of column names, each of which
    ## belonging to self.attributes, and returns a relation whose tuples are
    ## formed by projecting the columns from cnames.
    ## Example: R(A:INTEGER, B:INTEGER, C:DECIMAL) with tuples
    ## {(10,20,3.5),(11,22,7.8),(10,25,3.5)}
    ## Then, with cnames = ["A","C"], the output relation should
    ## have schema (A:INTEGER, C:DECIMAL) and tuples
    ## {(10,3.5),(11,7.8)}
    ## Note that after projection one may get duplicate tuples, which should
    ## be removed.
    def project(self, cnames):
        attr = cnames.copy()
        doms = []
        for column in cnames:
            doms.append(self.domains[self.attributes.index(column)])

        newRel = Relation("ANSWER", attr, doms)

        for tuple in self.table:
            newRel.addTuple(tuple.project(attr))

        newRel.removeDuplicates()

        return newRel

    # This method takes as input a comparison condition as explained earlier and returns
    # a new relation that contains only those tuples that satisfies the comparison condition.
    def select(self, lopType, lopValue, comparison, ropType, ropValue):
        newRel = Relation("ANSWER", self.attributes.copy(), self.domains.copy())

        for tuple in self.table:
            if tuple.select(lopType, lopValue, comparison, ropType, ropValue):
                newRel.addTuple(tuple)

        return newRel

    ## The join operator combines two relations into one based on common columns in the two relations
    ## The schema of the join relation contains all columns of the first relation followed by all columns
    ## of the second relation, somewhat like the times operator, except that the common columns appear only
    ## once in the join relation (keep first occurrence)
    ## Two tuples join with each other only if they have the same values under the common columns.
    def join(self, r2):
        ## Construct empty array lists for attr and dom
        ## copy all attributes and corresponding domains from this.attributes and this.domains to attr and dom
        ## copy attributes and corresponding domains from r2.attributes and r2.domains only if they do not appear
        ##    in this.attributes
        ## Construct new relation object, rel
        ## Using nested for loops obtain cloned tuple t1 from this.table and cloned tuple t2 from r2.table
        ##   and try to join these two tuples; if result is not None, add to rel
        ## return rel
        attr = self.attributes.copy()
        dom = self.domains.copy()

        for i in range(len(r2.attributes)):
            if not r2.attributes[i] in attr:
                attr.append(r2.attributes[i])
                dom.append(r2.domains[i])

        rel = Relation("ANSWER", attr, dom)

        aJoinedTuple = 0

        for tup in self.table:
            for fakeTup in r2.table:
                aJoinedTuple = tup.join(fakeTup)
                if not aJoinedTuple == None:
                    aJoinedTuple.attributes = attr
                    aJoinedTuple.domains = dom
                    rel.addTuple(aJoinedTuple)

        return rel

    def get_attributes(self):
        return self.attributes

    def get_domains(self):
        return self.domains

    # Return String version of relation; See output of run for format.
    def __str__(self):
        tuples = ''

        for tuple in self.table:
            tuples += str(tuple) + '\n'

        return self.displaySchema() + '\n' + "Number of tuples:" + str(len(self.table)) + '\n\n' + tuples
