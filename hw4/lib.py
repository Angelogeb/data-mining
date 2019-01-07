import numpy as np

from keras.models import Sequential
from keras.layers.recurrent import LSTM
from keras.layers.core import Dense, Activation
from keras.losses import mean_absolute_error


def simple_model_gen(in_timesteps, out_timesteps, feat_size):
    return Sequential([LSTM(out_timesteps, input_shape=(in_timesteps, feat_size))])


class LSTM_model(object):
    def __init__(
        self,
        in_timesteps,
        out_timesteps,
        x_cols,
        y_cols,
        step_col,
        train_ratio=0.7,
        model=simple_model_gen,
    ):
        self.in_timesteps = in_timesteps
        self.out_timesteps = out_timesteps
        self.x_cols = x_cols
        self.y_cols = y_cols
        self.step_col = step_col
        self.train_ratio = train_ratio
        self.model = model(in_timesteps, out_timesteps, len(x_cols))
        self.model.compile(loss="mean_absolute_error", optimizer="adam")

    def data(self, df):
        """Given a dataframe in input returns the X and Y extracted
        from the dataframe based on the columns and parameters this
        object has been initialized on
        
        Arguments:
            df {pandas.DataFrame}
        
        Returns:
            (X, Y)
        """
        self.steps = []
        X = []
        Y = []

        for i in range(len(df) - self.in_timesteps - self.out_timesteps + 1):
            X.append(df[self.x_cols].iloc[i : i + self.in_timesteps].values)
            Y.append(
                df[self.y_cols]
                .iloc[
                    i + self.in_timesteps : i + self.in_timesteps + self.out_timesteps
                ]
                .values.reshape(-1)
            )
            self.steps.append(
                df[self.step_col]
                .iloc[
                    i + self.in_timesteps : i + self.in_timesteps + self.out_timesteps
                ]
                .values
            )

        self.steps = np.array(self.steps)[:: self.out_timesteps].reshape(-1)

        Y = np.array(Y)

        self.orig_X = np.array(X)
        self.orig_Y = Y.reshape(-1, 1) if self.out_timesteps == 1 else Y

        self.X = np.apply_along_axis(lambda w: w / w[0] - 1, 1, self.orig_X)
        self.Y = (self.orig_Y / self.orig_X[:, 0, 2].reshape((-1, 1))) - 1

        self.plot_Y = self.Y[:: self.out_timesteps].reshape(-1)

        self.split = int(len(self.X) * self.train_ratio)

        self.orig_train = self.orig_X[:self.split], self.orig_Y[:self.split]
        self.orig_test = self.orig_X[self.split:], self.orig_Y[self.split:]

        self.train = self.X[:self.split], self.Y[:self.split]
        self.test = self.X[self.split:], self.Y[self.split:]

        return self

    def fit(self, epochs=500, batch_size=256):
        self.history = self.model.fit(
            self.train[0], self.train[1], epochs=epochs, batch_size=batch_size
        )
        return self

    def _get_orig_predicted(self, Y_pred, orig_X):
        return (Y_pred + 1) * orig_X[:, 0, 2][:, np.newaxis]

    def get_pred_test_pair(self):
        pred = self._get_orig_predicted(self.model.predict(self.test[0]), self.orig_test[0])[
            :: self.out_timesteps
        ].reshape(-1)
        timesteps = self.steps[self.split:]
        timesteps = timesteps[:len(timesteps) - (len(timesteps) - len(pred))]
        return timesteps, pred, self.orig_test[1][::self.out_timesteps].reshape(-1)

    def get_pred_train_pair(self):
        pred = self._get_orig_predicted(
            self.model.predict(self.train[0]), self.orig_train[0]
        )[:: self.out_timesteps].reshape(-1)
        timesteps = self.steps[:self.split]
        timesteps = timesteps[:len(self.steps) - (len(self.steps) - len(pred))]
        return timesteps, pred, self.orig_train[1][::self.out_timesteps].reshape(-1)

    def get_test_mae(self):
        _, pred, true = self.get_pred_test_pair()
        return np.abs(pred - true).mean()
    
    def get_train_mae(self):
        _, pred, true = self.get_pred_train_pair()
        return np.abs(pred - true).mean()