class Tuple:
    def __init__(self, attributes, domains):
        self.attributes = [a.upper() for a in attributes]
        self.domains = [d.upper() for d in domains]
        self.tuple = []

    # Add a tuple component to the end of the tuple
    def addComponent(self, comp):
        self.tuple.append(comp)

    # Return True if this tuple is equal to compareTuple; False otherwise
    # make sure the schemas are the same; return False if schema's are not same
    def equals(self, compareTuple):
        if self.__str__() == compareTuple.__str__():
            return True
        return False

    # clone this tuple with the attributes given.
    def clone(self, attr):
        cloneTup = Tuple(attr, self.domains)
        cloneTup.attributes = attr

        for component in self.tuple:
            cloneTup.addComponent(component)

        return cloneTup

    ## This method combines two tuples into one and assigns a new schema to the
    ## result tuple; the method returns the new tuple
    ## e.g. t1 = <"jones",20,200> and t2=<1,2,2.5>
    ## then t1.concatenate(t2,attr,dom) will be <"jones",20,200,1,2,2.5>
    ## with schema attr = <A, R.B, R.C, S.B, S.C, D>
    ## and dom = <VARCHAR,INTEGER,INTEGER,INTEGER,INTEGER,DECIMAL>
    def concatenate(self, t, attrs, doms):
        cloneTup = Tuple(attrs, doms)

        for comp in self.tuple:
            cloneTup.addComponent(comp)

        for comp in t.tuple:
            cloneTup.addComponent(comp)

        return cloneTup

    ## This method takes as input a list of column names, each of which
    ## belonging to self.attributes, and returns a new tuple with only those
    ## components that correspond to the column names in cnames.
    def project(self, cnames):
        doms = []
        for column in cnames:
            doms.append(self.domains[self.attributes.index(column)])

        tup = Tuple(cnames, doms)

        for column in cnames:
            tup.addComponent(self.tuple[self.attributes.index(column)])

        return tup

    # This method takes a comparison condition in the 5 parameters and
    # returns True if the tuple satisfies the condition and False otherwise.
    #
    # The comparison condition is coded in the 5 parameters as follows:
    #
    # lopType/ropType can take one of three values: "col", "num", "str"
    # indicating that the operand is either a name of a column, or a number,
    # or a string respectively.
    #
    # lopValue/ropValue will contain the name of the column if the lopType/ropType
    # is "col" and will contain a numeric value if lopType/ropType is "num" and
    # will contain a string value if lopType/ropType is "str".
    #
    # comparison will have one of six values: "<", "<=","=",">",">=", or "<>"
    #
    # As an example, if we want to express the comparison, SNAME = "Jones", the 5 parameters will be:
    # lopType="col", lopValue="SNAME", comparison="=", ropType="str", ropValue="Jones"
    #
    # As another example, if we want to express the condition GPA > 3.0, the 5 parameters will be:
    # lopType="col", lopValue="GPA", comparison=">", ropType="num", ropValue="3.0"
    #
    def select(self, lopType, lopValue, comparison, ropType, ropValue):
        # Top level cases to consider:
        #
        # lopType="num" and ropType="num"
        # lopType="str" and ropType="str"
        # lopType="col" and ropType="num"
        # lopType="col" and ropType="str"
        # lopType="num" and ropType="col"
        # lopType="str" and ropType="col"
        # lopType="col" and ropType="col"

        if lopType == 'col':
            if lopValue in self.attributes:
                tup = self.project([lopValue])

                if ropType == 'str':
                    if comparison == '<':
                        return tup.tuple[0] < ropValue
                    if comparison == '<=':
                        return tup.tuple[0] <= ropValue
                    if comparison == '=':
                        return tup.tuple[0] == ropValue
                    if comparison == '>':
                        return tup.tuple[0] > ropValue
                    if comparison == '>=':
                        return tup.tuple[0] >= ropValue
                    if comparison == '<>':
                        return tup.tuple[0] != ropValue

                if ropType == 'col':
                    rup = self.project([ropValue])
                    if comparison == '<':
                        return tup.tuple[0] < rup.tuple[0]
                    if comparison == '<=':
                        return tup.tuple[0] <= rup.tuple[0]
                    if comparison == '=':
                        return tup.tuple[0] == rup.tuple[0]
                    if comparison == '>':
                        return tup.tuple[0] > rup.tuple[0]
                    if comparison == '>=':
                        return tup.tuple[0] >= rup.tuple[0]
                    if comparison == '<>':
                        return tup.tuple[0] != rup.tuple[0]

                if ropType == 'num':
                    if comparison == '<':
                        return int(tup.tuple[0]) < int(ropValue)
                    if comparison == '<=':
                        return int(tup.tuple[0]) <= int(ropValue)
                    if comparison == '=':
                        return int(tup.tuple[0]) == int(ropValue)
                    if comparison == '>':
                        return int(tup.tuple[0]) > int(ropValue)
                    if comparison == '>=':
                        return int(tup.tuple[0]) >= int(ropValue)
                    if comparison == '<>':
                        return int(tup.tuple[0]) != int(ropValue)

        if lopType == 'num':
            if lopValue in self.tuple:
                if ropType == 'col':
                    tup = self.project([ropValue])

                    if comparison == '<':
                        return lopValue < tup.tuple[0]
                    if comparison == '<=':
                        return lopValue <= tup.tuple[0]
                    if comparison == '=':
                        return lopValue == tup.tuple[0]
                    if comparison == '>':
                        return lopValue > tup.tuple[0]
                    if comparison == '>=':
                        return lopValue >= tup.tuple[0]
                    if comparison == '<>':
                        return lopValue != tup.tuple[0]

                if ropType == 'num':
                    if comparison == '<':
                        return lopValue < ropValue
                    if comparison == '<=':
                        return lopValue <= ropValue
                    if comparison == '=':
                        return lopValue == ropValue
                    if comparison == '>':
                        return lopValue > ropValue
                    if comparison == '>=':
                        return lopValue >= ropValue
                    if comparison == '<>':
                        return lopValue != ropValue

        if lopType == 'str':
            if lopValue in self.tuple:
                if ropType == 'col':
                    tup = self.project([ropValue])

                    if comparison == '<':
                        return lopValue < tup.tuple[0]
                    if comparison == '<=':
                        return lopValue <= tup.tuple[0]
                    if comparison == '=':
                        return lopValue == tup.tuple[0]
                    if comparison == '>':
                        return lopValue > tup.tuple[0]
                    if comparison == '>=':
                        return lopValue >= tup.tuple[0]
                    if comparison == '<>':
                        return lopValue != tup.tuple[0]

                if ropType == 'str':
                    if comparison == '<':
                        return lopValue < ropValue
                    if comparison == '<=':
                        return lopValue <= ropValue
                    if comparison == '=':
                        return lopValue == ropValue
                    if comparison == '>':
                        return lopValue > ropValue
                    if comparison == '>=':
                        return lopValue >= ropValue
                    if comparison == '<>':
                        return lopValue != ropValue

    ## This method attempts to construct a "joined" tuple out of this.tuple and t2.tuple
    ## If the two tuples can join, the joined tuple is returned; otherwise None is returned.
    ## Let ENROLL(SID:INTEGER,SNAME:VARCHAR,PHONE:INTEGER,MAJOR:VARCHAR,GPA:DECIMAL)
    ## and STUDENT(SID:INTEGER,COURSE:VARCHAR,GRADE:VARCHAR)
    ## let this.tuple = 1111:Robert Adams:1234:Computer Science:4.0:
    ## and   t2.tuple = 1111:Database Systems:A:
    ## Then, joined tuple will be: 1111:Robert Adams:1234:Computer Science:4.0:Database Systems:A:
    ## As another example,
    ## let this.tuple = 1111:Robert Adams:1234:Computer Science:4.0:
    ## and   t2.tuple = 1114:Database Systems:B:
    ## These two tuples do not join because in the first tuple SID=1111 and in the second
    ## tuple SID=1114; So, the result should be None
    def join(self, t2):
        ## collect information about "common" attributes and their positions in the respective lists.
        ## Verify if the two tuples can join; if not return None
        ## If tuples can join then produce the joined tuple and return it.

        commonAttr = []
        commonDoms = []
        commonIndices = []
        for attr in self.attributes:
            if attr in t2.attributes:
                commonAttr.append(attr)
                commonIndices.append(t2.attributes.index(attr))

        checks = []

        for i in range(len(commonAttr)):
            if True:  # commonIndices[i] == self.attributes.index(commonAttr[i]):
                checks.append(True)
            else:
                checks.append(False)

        if False in checks:
            return None

        for i in range(len(commonAttr)):
            ropType = ""

            if t2.domains[commonIndices[i]] == "INTEGER" or t2.domains[commonIndices[i]] == "DECIMAL":
                ropType = "num"

            if t2.domains[commonIndices[i]] == "VARCHAR":
                ropType = "str"

            if not self.select("col", commonAttr[i], "=", ropType, t2.tuple[commonIndices[i]]):
                return None

        cnames = []
        doms = []
        for i in range(len(t2.attributes)):
            if i in commonIndices:
                pass
            else:
                cnames.append(t2.attributes[i])
                doms.append(t2.domains[i])

        tup = t2.project(cnames)

        joinedTup = self.clone(self.attributes).concatenate(tup, cnames, doms)

        return joinedTup

    # Return String representation of tuple; See output of run for format.
    def __str__(self):
        endString = ''

        for value in self.tuple:
            endString += str(value) + ':'

        return endString
