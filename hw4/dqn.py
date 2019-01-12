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
        self.training = True

    def _build_model(self):
        # Neural Net for Deep-Q learning Model
        model = Sequential()
        model.add(Dense(24, input_dim=self.state_size + 2, activation="relu"))
        model.add(Dropout(0.5))
        model.add(Dense(24, activation="relu"))
        model.add(Dropout(0.5))
        model.add(Dense(self.action_size, activation="linear"))
        model.compile(loss="mse", optimizer=Adam(lr=self.learning_rate))
        return model

    def remember(self, state, action, reward, next_state, done):
        self.memory.append((state, action, reward, next_state, done))

    def act(self, state):
        if self.training and np.random.rand() <= self.epsilon:
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
        self.BTC_AMOUNT = 0.5
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
        return np.append(self.norm_windows[self.curr_window], [self.budget, self.btc])

    def step(self, action):
        old_price = self.windows[self.curr_window][-1]
        invalid_action = False
        self.curr_window += 1
        curr_price = self.windows[self.curr_window][-1]


        reward_buying = (self.BTC_AMOUNT * curr_price) - (
            self.BTC_AMOUNT * old_price
        )
        reward_selling = (self.BTC_AMOUNT * old_price) - (
            self.BTC_AMOUNT * curr_price
        )

        best_reward = max([reward_buying, reward_selling])
        reward = -best_reward

        if action == 0:
            # buying
            if self.budget < self.BTC_AMOUNT * old_price:
                invalid_action = True
            else:
                self.btc += self.BTC_AMOUNT
                self.budget -= self.BTC_AMOUNT * old_price
                reward += reward_buying
        elif action == 1:
            # selling
            if self.btc < self.BTC_AMOUNT:
                invalid_action = True
            else:
                self.budget += self.BTC_AMOUNT * old_price
                self.btc -= self.BTC_AMOUNT
                reward += reward_selling

        done = self.curr_window == len(self.windows) - 1

        return np.append(self.norm_windows[self.curr_window], [self.budget, self.btc]), reward, done

