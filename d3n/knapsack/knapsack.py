#!/usr/bin/python

def knapsack_fractional_01():
    print("01_fractional_knapsack")
    return 0;

def knapsack_01(W, wt, val, n):
    print("01_knapsack")
    knapsack = [[0 for x in range(W+1)] for x in range(n+1)]

    # Build table K[][] in bottom up manner
    for i in range(n+1):
        for w in range(W+1):
            if i==0 or w==0:
                knapsack[i][w] = 0
            elif wt[i-1] <= w:
                knapsack[i][w] = max(val[i-1] + knapsack[i-1][w-wt[i-1]],  knapsack[i-1][w])
            else:
                knapsack[i][w] = knapsack[i-1][w]
                
    return knapsack[n][W], knapsack


# knapsack of capacity W
def knapsack_01_print(W, wt, val, n):
    max_val, knapsack = knapsack_01(W, wt, val, n)

    print('Knapsack Value', max_val)
    K = knapsack
    res = max_val
    w = W
    for i in range(n, 0, -1):
        if res <= 0:
            break
        
        # either the result comes from the
	# top (K[i-1][w]) or from (val[i-1]
	# + K[i-1] [w-wt[i-1]]) as in Knapsack
	# table. If it comes from the latter
	# one/ it means the item is included.
        if res == K[i - 1][w]:
            continue
        else:
            # This item is included.
            print(wt[i - 1])
            
            # Since this weight is included
            # its value is deducted
            res = res - val[i - 1]
            w = w - wt[i - 1]

# Driver code
wt = [8, 9, 7]
val = [4, 5, 6]
W = 20
n = len(val)

knapsack_01_print(W, wt, val, n)
