import sys
filename = sys.argv[1]
def generate_big_random_letters(filename,size):
    """
    generate big random letters/alphabets to a file
    :param filename: the filename
    :param size: the size in bytes
    :return: void
    """
    import random
    import string

    chars = ''.join([random.choice(string.ascii_letters) for i in range(size)]) #1


    with open(filename, 'w') as f:
        f.write(chars)

generate_big_random_letters(filename, 10*1024*1024)
