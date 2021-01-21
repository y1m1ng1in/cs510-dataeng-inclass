import json

def main():
    file = open("sample")
    content = file.read()
    parsed = json.loads(content)
    choosed = parsed[:1000]
    file.close()

    with open('bcsample.json', 'w') as outfile:
        json.dump(choosed, outfile)
        outfile.close()

if __name__ == '__main__':
    main()