from collections import Counter
from random import shuffle

ace_in_first_two = lambda cards: cards[0][0] == 1 or cards[1][0] == 1
ace_in_first_five = lambda cards: 1 in [c[0] for c in cards[:5]]
same_rank_first_two = lambda cards: cards[0][0] == cards[1][0]
all_diamonds_first_five = lambda cards: "".join([c[1] for c in cards[:5]]) == (5*"D")

def full_house_first_five(cards):
    c = Counter([c[0] for c in cards[:5]])
    tmp = c.values()
    return 3 in tmp and 2 in tmp

def prob_of(positive, gen_sample, n_trials):
    pos = 0
    for i in range(n_trials):
        if positive(gen_sample()):
            pos += 1
    return pos/n_trials

cards = [(rank, suit) for rank in range(1,14) for suit in ["H", "D", "S", "C"]]

def sample_gen():
    shuffle(cards)
    return cards

n_trials = 10**5

processes = [ace_in_first_two,
             ace_in_first_five,
             same_rank_first_two,
             all_diamonds_first_five,
             full_house_first_five]

my_results = [     33/221,
              18472/54145,
                     3/51,
             1287/2598960,
                   6/4165]

print('ex)', '{:>14}'.format("simulated"), '{:>15}'.format("found") )

for (ex, proc, res) in zip(["a", "b", "c", "d", "e"], processes, my_results):
    prob = prob_of(proc, sample_gen, n_trials)
    print(ex + ")", '{:>15.4f}'.format(prob), '{:>15.4f}'.format(res))
