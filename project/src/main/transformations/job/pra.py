strs = ["eat","tea","tan","ate","nat","bat"]
a = []
d = {}
def function():
    for i in strs:
        print(i)
        for j in i:
            if j in d:
                a.append(d)
                print(a)
            else:
                d[j] =1


print(function())