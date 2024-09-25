import sys
from RAParser import parser
from Database import *
from Relation import *
from Tuple import *
from Node import *
from phase2.Integer import Integer


def execute_file(filename, db):
    try:
        with open(filename) as f:
            data = f.read().splitlines();
        result = " ".join(list(filter(lambda x: len(x) > 0 and x[0] != "#", data)))
        try:
            tree = parser.parse(result)
            set_temp_table_names(tree)
            msg = semantic_checks(tree, db)
            if msg == 'OK':
                print()
                print(evaluate_query(tree, db))
                print()
            else:
                print(msg)
        except Exception as inst:
            print(inst.args[0])
    except FileNotFoundError:
        print("FileNotFoundError: A file with name " + "'" + \
              filename + "' cannot be found")


def read_input():
    result = ''
    data = input('RA: ').strip()
    while True:
        if ';' in data:
            i = data.index(';')
            result += data[0:i + 1]
            break
        else:
            result += data + ' '
            data = input('> ').strip()
    return result


def set_temp_table_names(tree: Node, counter: Integer):
    if tree.left_child is None and tree.right_child is None:
        if tree.node_type != 'relation':
            tree.relation_name = "temp" + str(counter)
            counter.increment()
        return

    if tree.left_child:
        set_temp_table_names(tree.left_child, counter)
        tree.relation_name = "temp" + str(counter)
        counter.increment()
        return
    if tree.right_child:
        set_temp_table_names(tree.right_child, counter)
        tree.relation_name = "temp" + str(counter)
        counter.increment()
        return



# perform semantic checks; set tree.attributes and tree.domains along the way
# return "OK" or ERROR message
def semantic_checks(tree: Node, db: Database):
    if tree.node_type == "relation":
        if tree.left_child == None and tree.right_child == None:
            if db.getRelation(tree.relation_name) == None:
                return f'Relation {tree.relation_name} does not exist'
            else:
                relation = db.getRelation(tree.relation_name)
                tree.set_attributes(relation.get_attributes())
                tree.set_domains(relation.get_domains())
                return 'OK'
    if tree.node_type == "union" or tree.node_type == "intersect" or tree.node_type == "minus":
        leftrelationToCheck = semantic_checks(tree.left_child, db)
        rightRelationToCheck = semantic_checks(tree.right_child, db)

        if leftrelationToCheck != 'OK':
            return leftrelationToCheck
        elif rightRelationToCheck != 'OK':
            return rightRelationToCheck

        if len(tree.left_child.attributes) != len(tree.right_child.attributes):
            return f'SEMANTIC ERROR (UNION/INTERSECT/MINUS): Relational Attributes not equal in number.'

        if tree.left_child.domains != tree.right_child.domains:
            return f'SEMANTIC ERROR (UNION/INTERSECT/MINUS): Relational Domains do not have equal data types'

        tree.set_attributes(tree.left_child.attributes)
        tree.set_domains(tree.left_child.domains)
        return 'OK'
    if tree.node_type == "project":
        columnList = tree.columns
        projectedRelation = semantic_checks(tree.left_child, db)

        if projectedRelation != 'OK':
            return projectedRelation

        for column in columnList:
            if column not in tree.left_child.attributes:
                return f'SEMANTIC ERROR (PROJECT): attribute {column} does not exist'
            if columnList.count(column) > 1:
                return f'SEMANTIC ERROR (PROJECT): duplicate attribute'

        tree.set_attributes(tree.columns)
        domains = []
        for attr in tree.columns:
            domains.append(tree.left_child.domains[tree.left_child.attributes.index(attr)])
        tree.set_domains(domains)

        return 'OK'
    if tree.node_type == "rename":

        columnList = tree.columns
        projectedRelation = semantic_checks(tree.left_child, db)

        if projectedRelation != 'OK':
            return projectedRelation

        if len(columnList) != len(tree.left_child.attributes):
            return f'SEMANTIC ERROR (RENAME): invalid length of attributes'

        for column in columnList:
            if columnList.count(column) > 1:
                return f'SEMANTIC ERROR (RENAME): duplicate attribute'

        tree.set_attributes(tree.columns)
        tree.set_domains(tree.left_child.get_domains())

        return 'OK'

    if tree.node_type == 'select':
        conditions = tree.conditions
        selectCheck = semantic_checks(tree.left_child, db)

        if selectCheck != 'OK':
            return selectCheck
        for list in conditions:
            if list[0] == 'col':
                if list[1] not in tree.left_child.attributes:
                    return f'SEMANTIC ERROR (SELECT): left column name is invalid'
            if list[3] == 'col':
                if list[4] not in tree.left_child.attributes:
                    return f'SEMANTIC ERROR (SELECT): right column name is invalid'

            if list[0] == 'col' and list[3] == 'col':
                if tree.left_child.domains[tree.left_child.attributes.index(list[1])] != tree.left_child.domains[tree.left_child.attributes.index(list[4])]:
                    return f'SEMANTIC ERROR (SELECT): data types do not match'


        tree.set_attributes(tree.left_child.get_attributes())
        tree.set_domains(tree.left_child.get_domains())
        return 'OK'

    if tree.node_type == 'join':
        leftJoinCheck = semantic_checks(tree.left_child, db)
        rightJoinCheck = semantic_checks(tree.right_child, db)

        if leftJoinCheck != 'OK':
            return leftJoinCheck
        if rightJoinCheck != 'OK':
            return rightJoinCheck

        attr = tree.left_child.attributes.copy()
        dom = tree.left_child.domains.copy()

        for attribute in attr:
            if attribute in tree.right_child.attributes:
                if dom[attr.index(attribute)] != tree.right_child.domains[tree.right_child.attributes.index(attribute)]:
                    return f'SEMANTIC ERROR (JOIN): common column data type does not match'


        for i in range(len(tree.right_child.attributes)):
            if not tree.right_child.attributes[i] in attr:
                attr.append(tree.right_child.attributes[i])
                dom.append(tree.right_child.domains[i])


        tree.set_attributes(attr)
        tree.set_domains(dom)
        return 'OK'
    if tree.node_type == 'times':
        leftTimesCheck = semantic_checks(tree.left_child, db)
        rightTimesCheck = semantic_checks(tree.right_child, db)

        if leftTimesCheck != 'OK':
            return leftTimesCheck
        if rightTimesCheck != 'OK':
            return rightTimesCheck

        newAttr = []
        newDoms = []

        for attr in tree.left_child.attributes:
            if attr in tree.right_child.attributes:
                newAttr.append(tree.left_child.relation_name + "." + attr)
            else:
                newAttr.append(attr)
            newDoms.append(tree.left_child.domains[tree.left_child.attributes.index(attr)])

        for attr in tree.right_child.attributes:
            if attr in tree.left_child.attributes:
                newAttr.append(tree.right_child.relation_name + "." + attr)
            else:
                newAttr.append(attr)
            newDoms.append(tree.right_child.domains[tree.right_child.attributes.index(attr)])


        tree.set_attributes(newAttr)
        tree.set_domains(newDoms)
        return 'OK'



def evaluate_query(tree: Node, db: Database):
    if tree.node_type == "relation":
        if tree.left_child is None or tree.right_child is None:
            return db.getRelation(tree.relation_name)
    elif tree.node_type == "project":
        columnList = tree.columns
        projectedRelation = evaluate_query(tree.left_child, db).project(columnList)
        return projectedRelation
    elif tree.node_type == "join":
        leftRelation = evaluate_query(tree.left_child, db)
        rightRelation = evaluate_query(tree.right_child, db)
        joinedRelation = leftRelation.join(rightRelation)
        return joinedRelation
    elif tree.node_type == "select":
        selectConditions = tree.conditions
        selectedConditionRelations = []
        for list in selectConditions:
            selectedConditionRelations.append(evaluate_query(tree.left_child, db).select(list[0],list[1],list[2],list[3],list[4]))

        if len(selectedConditionRelations) > 1:
            newRelation = selectedConditionRelations[0]
            selectedConditionRelations.remove(newRelation)
            multiSelect = newRelation
            for relation in selectedConditionRelations:
                multiSelect = multiSelect.intersect(relation)

            return multiSelect
        else:
            return selectedConditionRelations[0]
    elif tree.node_type == "rename":
        columnList = tree.columns
        renameRelation = evaluate_query(tree.left_child, db).rename(columnList)
        return renameRelation
    elif tree.node_type == "intersect":
        leftRelation = evaluate_query(tree.left_child, db)
        rightRelation = evaluate_query(tree.right_child, db)
        intersectedRelation = leftRelation.intersect(rightRelation)
        return intersectedRelation
    elif tree.node_type == "union":
        leftRelation = evaluate_query(tree.left_child, db)
        rightRelation = evaluate_query(tree.right_child, db)
        unionRelation = leftRelation.union(rightRelation)
        return unionRelation
    elif tree.node_type == "minus":
        leftRelation = evaluate_query(tree.left_child, db)
        rightRelation = evaluate_query(tree.right_child, db)
        minusRelation = leftRelation.minus(rightRelation)
        return minusRelation
    elif tree.node_type == "times":
        leftRelation = evaluate_query(tree.left_child, db)
        rightRelation = evaluate_query(tree.right_child, db)
        timesRelation = leftRelation.times(rightRelation)
        return timesRelation

def main():
    db = Database()
    db.initializeDatabase(sys.argv[1])

    while True:
        data = read_input()
        counter = Integer(0)
        if data == 'schema;':
            print(db.displaySchema())
            continue
        if data.strip().split()[0] == "source":
            filename = data.strip().split()[1][:-1]
            execute_file(filename, db)
            continue
        if data == 'help;' or data == "h;":
            print("\nschema; 		# to see schema")
            print("source filename; 	# to run query in file")
            print("exit; or quit; or q; 	# to exit\n")
            continue
        if data == 'exit;' or data == "quit;" or data == "q;":
            break
        try:
            tree = parser.parse(data)
        except Exception as inst:
            print(inst.args[0])
            continue
        print("********************************")
        tree.print_tree(0)
        print("********************************")
        set_temp_table_names(tree, counter)
        msg = semantic_checks(tree, db)
        # print("********************************")
        # tree.print_tree(0)
        # print("********************************")
        if msg == 'OK':
            # print('Passed semantic checks')
            print()
            print(evaluate_query(tree, db))
        else:
            print(msg)


if __name__ == '__main__':
    main()
