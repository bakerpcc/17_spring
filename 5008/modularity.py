from collections import deque

file=open('input_1.txt')

def init_matrix(file):
    matrix=[]
    cnt = 0
    while 1:
        line = file.readline()
        if not line:
            break
        #print('new_line')
        if cnt==0:
            size=int(line)

            #print(num)

            #print(V)
            #print(A)
        else:
            #print(line)
            #print(line[0],line[1],line[2])
            temp=[]
            for i in range(0,size):
                #print(line[2*i])
                temp.append(str(line[2*i]))
                #if line[2*i]=='1':
                #    A[str(cnt)].append(str(i+1))
                #    E.append([str(cnt),str(i+1)])
                    #print(A)
                    #print(E)
            #temp=line.split(' ',1)[0]
            #print(temp)
            matrix.append(temp)
        cnt=cnt+1
    return size,matrix

"""
num,matrix=init_matrix(file)

print(num)
print(matrix)
"""


def generator(matrix,num):
    V = []
    E = []
    for i in range(1, num + 1):
        V.append(str(i))
    A = dict((w, []) for w in V)
    for j in range(0,num):
        for k in range(0,num):
            if matrix[j][k]=='1':
                A[str(j+1)].append(str(k+1))
                E.append([str(j+1),str(k+1)])
    return V,E,A

"""
V,E,A=generator(matrix,num)


print(V)
print(len(E))
print(E)
print(A)
"""

#def connectness(V,E):


def brandes(V, A):
    "Compute betweenness centrality in an unweighted graph."
    # Brandes algorithm
    # see http://www.cs.ucc.ie/~rb4/resources/Brandes.pdf
    C = dict((v,0) for v in V)
    CE= dict(((v,w),0) for (v,w) in E)
    # print("C:")
    # print(C)
    for s in V:
        S = []
        P = dict((w,[]) for w in V)
        # print("P:")
        # print(P)
        g = dict((t, 0) for t in V); g[s] = 1
        # print("g:")
        # print(g)
        d = dict((t,-1) for t in V); d[s] = 0
        # print("d:")
        # print(d)
        Q = deque([])
        Q.append(s)
        # print("Q:")
        # print(Q)
        while Q:
            v = Q.popleft()
            # print("v:")
            # print(v)
            S.append(v)
            # print("S:")
            # print(S)
            for w in A[v]:
                if d[w] < 0:
                    Q.append(w)
                    d[w] = d[v] + 1
                    # print("dw:")
                    # print(d[w])
                if d[w] == d[v] + 1:
                    g[w] = g[w] + g[v]
                    P[w].append(v)
                    # print("pw:")
                    # print(P[w])
        e = dict((v, 0) for v in V)
        # print("e:")
        # print(e)
        while S:
            w = S.pop()
            for v in P[w]:
                #if ord(w)<ord(v):
                #    temp=v
                #    v=w
                #    w=temp
                CE[(v,w)]=CE[(v,w)]+(g[v]/g[w]) * (1 + e[w])
                e[v] = e[v] + (g[v]/g[w]) * (1 + e[w])
            if w != s:
                C[w] = C[w] + e[w]
    return C,CE
"""
#print(brandes(V,A))
C,CE=brandes(V,A)
#print(C)
#print(CE)
"""
def find_max_betweenness(CE):
    SE=[]
    for (v,w) in E:
        if ord(v)<ord(w):
            SE.append((v,w))
    #print(SE)
    #print(CE[('2','3')])
    max=0
    max_e=[]
    #print('start')
    for (v,w) in SE:
        #print((v,w))
        #print(CE[(v,w)])
        if round(CE[(v,w)],10)==round(max,10):
            max_e.append((v,w))
            #print((v,w))
        else:
            if CE[(v,w)]>max:
                max=CE[(v,w)]
                max_e=[]
                max_e.append((v, w))
    #print(max)
    #print(max_e)
    #print('find end')
    return max,max_e

"""
max,max_e=find_max_betweenness(CE)
print(max)
print(max_e)
"""

def del_edge(matrix,max_e):
    matrix[int(max_e[0])-1][int(max_e[1])-1]='0'
    matrix[int(max_e[1])-1][int(max_e[0])-1] = '0'
    return matrix
"""
new_matrix=del_edge(matrix,max_e)
print(new_matrix)


V,E,A=generator(matrix,num)

print(V)
print(len(E))
print(E)
print(A)

C,CE=brandes(V,A)
max,max_e=find_max_betweenness(CE)
print(max)
print(max_e)

"""

def dfs(graph, start, visited=None):
    if visited is None:
        visited = []
        q=deque([])
    visited.append(start)
    q.append(start)
    #print(visited)
    while q:
        v=q.popleft()
        for i in graph[v]:
            #print(i)
            if i not in visited:
                visited.append(i)
                q.append(i)
            #else:
            #    print("do nothing")
    #for next in list(set(graph[start]) - set(visited)):
    #    dfs(graph, next, visited)
    #print(visited)
    return visited
#print(dfs(l, '5'))

def init_community(v,l):
    rest=v
    #print(rest)
    community=[]
    while len(rest)>0:
        #print(len(rest))
        root = rest[0]
        community_tmp=dfs(l,root)
        #print(community_tmp)
        rest = list(set(rest) - set(community_tmp))
        #print(rest)
        community.append(community_tmp)
    return community

def cal_m(matrix,size):
    m=0
    for i in range(0,size):
        for j in range(0,size):
            if matrix[i][j]=='1':
                m=m+1
    #print(m)
    return m/2

def cal_degree(V,A):
    #print(len(A[V]))
    return len(A[V])

count=1
size,matrix=init_matrix(file)

V, E, A = generator(matrix, size)
community = init_community(V, A)
#print(community)

m_value=int(cal_m(matrix,size))
#print(m_value)

original_matrix=matrix
original_A=A
max_modularity=0;
while count<size:
    global_max=0;
    global_max_e=[];
    list_modularity=[];
    for c in community:
        C,CE=brandes(c,A)
        #print(CE)
        #cal betweenness and del max edge
        max, max_e = find_max_betweenness(CE)
        if(round(max,10)==round(global_max,10)):
            for (v,m) in max_e:
                global_max_e.append((v,m))
        else:
            if(max>global_max):
                global_max=max
                global_max_e=[]
                for (v, m) in max_e:
                    global_max_e.append((v, m))
    #print(global_max)
    #print(global_max_e)
    for (v,w) in global_max_e:
        #print((v,w))
        del_edge(matrix,(v,w))
    #count=count+1
    V, E, A = generator(matrix, size)
    community = init_community(V, A)
    # cal modularity
    sum_unit = 0
    for c in community:
        #print(c)
        for v1 in c:
            for v2 in c:
                #if (v1,v2) not in E:
                #    #print(type(cal_degree(v1,A)))
                #    #print(type(m))
                #    sum_unit=sum_unit+0-round(cal_degree(v1,A)*cal_degree(v2,A)/int(2*m),10)
                #else:
                #    sum_unit=sum_unit+matrix[v1][v2]-round(cal_degree(v1,A)*cal_degree(v2,A)/(2*m),10)
                #print(v1,v2)
                #print(matrix[int(v1)-1][int(v2)-1])
                #print(cal_degree(v1,A))
                #print(cal_degree(v2, A))
                #print(2*m_value)
                #print(round(cal_degree(v1,A)*cal_degree(v2,A)/int(2*m_value),10)
                temp=int(original_matrix[int(v1)-1][int(v2)-1])-round(cal_degree(v1,original_A)*cal_degree(v2,original_A)/int(2*m_value),10)
                sum_unit=sum_unit+temp
                #print(temp)
                #print(sum_unit)
                #print("===")
    #print(sum_unit)
    sum_unit=sum_unit/(2*m_value)
    #print(sum_unit)
    #print(max_modularity)
    list_modularity.append(sum_unit)
    if sum_unit>max_modularity:
        max_modularity=sum_unit
        optimal_structure = community
    print(community)
    count=len(community)
    print("%d clusters: modularity %f"%(count,sum_unit))

print("optimal structure:",optimal_structure)


"""
V, E, A = generator(matrix, size)
community = init_community(V, A)
print(community)
"""