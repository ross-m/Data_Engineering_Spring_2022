fp = open("bcsample.json", "w")
fp.write("[\n")

for i in range(1, 1001):
    w = "{\"event number\": \"%d\"},\n" % i
    fp.write(w)

fp.write("]\n")
fp.close()
