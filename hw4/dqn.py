# -*- coding: utf-8 -*-
import random
import gym
import numpy as np
from collections import deque
from keras.models import Sequential
from keras.layers import Dense, Dropout
from keras.optimizers import Adam

EPISODES = 1000


class DQNAgent:
    def __init__(self, state_size, action_size):
        self.state_size = state_size
        self.action_size = action_size
        self.memory = deque(maxlen=2000)
        self.gamma = 0.95  # discount rate
        self.epsilon = 1.0  # exploration rate
        self.epsilon_min = 0.01
        self.epsilon_decay = 0.995
        self.learning_rate = 0.001
        self.model = self._build_model()

    def _build_model(self):
        # Neural Net for Deep-Q learning Model
        model = Sequential()
        model.add(Dense(24, input_dim=self.state_size, activation="relu"))
        model.add(Dropout(0.5))
        model.add(Dense(24, activation="relu"))
        model.add(Dropout(0.5))
        model.add(Dense(self.action_size, activation="linear"))
        model.compile(loss="mse", optimizer=Adam(lr=self.learning_rate))
        return model

    def remember(self, state, action, reward, next_state, done):
        self.memory.append((state, action, reward, next_state, done))

    def act(self, state):
        if np.random.rand() <= self.epsilon:
            return random.randrange(self.action_size)
        act_values = self.model.predict(state)
        return np.argmax(act_values[0])  # returns action

    def replay(self, batch_size):
        minibatch = random.sample(self.memory, batch_size)
        for state, action, reward, next_state, done in minibatch:
            target = reward
            if not done:
                target = reward + self.gamma * np.amax(
                    self.model.predict(next_state)[0]
                )
            target_f = self.model.predict(state)
            target_f[0][action] = target
            self.model.fit(state, target_f, epochs=1, verbose=0)
        if self.epsilon > self.epsilon_min:
            self.epsilon *= self.epsilon_decay

    def load(self, name):
        self.model.load_weights(name)

    def save(self, name):
        self.model.save_weights(name)


class BitcoinStockEnvironment:
    def __init__(self, data, budget, w_size):
        self.windows = None
        self.curr_window = 0
        self.initial_budget = budget
        self.budget = budget
        self.btc = 0
        self.current_liquidity = budget
        self.windows = []
        n = len(data)
        for i in range(n - w_size + 1):
            self.windows.append(data[i : i + w_size])
        self.windows = np.array(self.windows)
        self.norm_windows = self.windows / self.windows[:, 0].reshape((-1, 1)) - 1

    def reset(self):
        self.curr_window = 0
        self.budget = self.initial_budget
        self.btc = 0
        self.current_liquidity = self.budget
        return self.norm_windows[self.curr_window]

    # Actions:
    #   0: do nothing
    #   1: buy everything
    #   2: sell everything

    def get_best_liquidity(self):
        reward_buying = (
            self.budget
            / self.windows[self.curr_window][-1]
            * self.windows[self.curr_window + 1][-1]
        )
        reward_selling = self.btc * self.windows[self.curr_window][-1]
        reward_doing_nothing = (
            self.btc * self.windows[self.curr_window + 1][-1] + self.budget
        )
        return max([reward_buying, reward_selling, reward_doing_nothing])

    def step(self, action):
        old_price = self.windows[self.curr_window][-1]
        invalid_action = False
        best_liquidity = self.get_best_liquidity()
        if action == 1:
            # all in buying
            if self.budget == 0:
                invalid_action = True
            self.btc += self.budget / old_price
            self.budget = 0
        elif action == 2:
            # all in selling
            if self.btc == 0:
                invalid_action = True
            self.budget += self.btc * old_price
            self.btc = 0

        self.curr_window += 1
        curr_price = self.windows[self.curr_window][-1]

        new_liquidity = self.btc * curr_price + self.budget

        reward = new_liquidity / best_liquidity
        if invalid_action:
            reward /= 2

        self.current_liquidity = new_liquidity

        done = self.curr_window == len(self.windows) - 1

        return self.norm_windows[self.curr_window], reward, done


if __name__ == "__main__":
    env = gym.make("CartPole-v1")
    state_size = env.observation_space.shape[0]
    action_size = env.action_space.n
    agent = DQNAgent(state_size, action_size)
    # agent.load("./save/cartpole-dqn.h5")
    done = False
    batch_size = 32

    for e in range(EPISODES):
        state = env.reset()
        state = np.reshape(state, [1, state_size])
        for time in range(500):
            # env.render()
            action = agent.act(state)
            next_state, reward, done, _ = env.step(action)
            reward = reward if not done else -10
            next_state = np.reshape(next_state, [1, state_size])
            agent.remember(state, action, reward, next_state, done)
            state = next_state
            if done:
                print(
                    "episode: {}/{}, score: {}, e: {:.2}".format(
                        e, EPISODES, time, agent.epsilon
                    )
                )
                break
            if len(agent.memory) > batch_size:
                agent.replay(batch_size)
        # if e % 10 == 0:
        #     agent.save("./save/cartpole-dqn.h5")
