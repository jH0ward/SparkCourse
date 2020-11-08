from pyspark import SparkConf, SparkContext

# Boilerplate spark
conf = SparkConf().setMaster("local[*]").setAppName("SparkBFS")
sc = SparkContext(conf=conf)

# Characters of interest
startCharacterID = 5306
targetCharacterID = 14

# Initialize accumulator
hitCounter = sc.accumulator(0)


def convert_line_to_bfs(line):
    ary = line.split()
    hero_id = int(ary[0])

    connections = []
    for c in ary[1:]:
        connections.append(int(c))

    color = 'WHITE'
    distance = 9999

    if hero_id == startCharacterID:
        distance = 0
        color = 'GRAY'

    return hero_id, (connections, distance, color)


def bfs_map(node):
    hero_id = node[0]
    connects = node[1][0]
    distance = node[1][1]
    color = node[1][2]

    # Visit each connection
    results = []

    # Only expand gray vertices
    if color == 'GRAY':
        color = 'BLACK'  # Now you're black
        for c in connects:
            new_hero_id = c
            new_color = 'GRAY'  # Each visited node turned gray
            new_distance = distance + 1  # You're plus 1 farther away from the start node
            if targetCharacterID == new_hero_id:
                hitCounter.add(1)
                print(f'Got it from {hero_id} w/ distance = {new_distance}')

            # Record visited nodes' distance and color to be reduced later
            results.append((new_hero_id, ([], new_distance, new_color)))

    # Emit the initial node because it has a new color and we need to keep it
    results.append((hero_id, (connects, distance, color)))
    return results
# convert list of id and connections to id, (conn_list), distance, color
# 5989 1165 3836 4361 1282 --> 5989, (1165, 3836, 4361, 1281), 9999, White


def red_bff(data1, data2):
    # Idea is to keep darkest node and min distance
    edges1, distance1, color1 = data1
    edges2, distance2, color2 = data2
    distance = 9999
    edges = []
    color = 'WHITE'

    # Preserve min distance for this key
    if distance1 < distance:
        distance = distance1
    if distance2 < distance:
        distance = distance2

    # Preserve all discovered edges
    if len(edges1) > 0:
        edges.extend(edges1)
    if len(edges2) > 0:
        edges.extend(edges2)

    # Keep darkest color
    color_set_list = list({color, color1, color2})
    if 'BLACK' in color_set_list:
        color = 'BLACK'
    elif 'GRAY' in color_set_list:
        color = 'GRAY'
    else:
        color = 'WHITE'

    return edges, distance, color


lines = sc.textFile("../data/Marvel+Graph")

# We start with only the startCharacterID node as gray
rdd = lines.map(convert_line_to_bfs)
initial_rdd = rdd
print(rdd.take(1))

for i in range(5):
    print(f'Running iteration {i}')
    # For the map function, we explore all neighbors and turn them gray
    # IOW we turn grays black (only 1 node the starter on first iteration) and their connections from white to gray
    # If the target is found, we signal in the accumulator
    rdd = rdd.flatMap(bfs_map)
    print(f'Processing {rdd.count()} values...')

    if hitCounter.value > 0:
        print(f'Got {hitCounter.value} hits')

        break

    # Reduce by key combines each character ID data and keeps the darkest color and shortest path
    rdd = rdd.reduceByKey(red_bff)

l = list(rdd.take(10))

for a in l:
    print(a)


#initial_rdd = initial_rdd.reduceByKey(lambda x, y: x+y)
initial_rdd = initial_rdd.map(lambda x: (x[0], len(x[1][0])))
print(initial_rdd.take(1))
tst = initial_rdd.filter(lambda x: x[0] == 5306).collect()
print(tst)
