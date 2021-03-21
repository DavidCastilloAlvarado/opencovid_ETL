import pandas as pd
import numpy as np
import os
from tqdm import tqdm
from matplotlib import pyplot as plt
from matplotlib.dates import date2num, num2date
from matplotlib import dates as mdates
from matplotlib import ticker
from matplotlib.colors import ListedColormap
from matplotlib.patches import Patch
from scipy import stats as sps
from scipy.interpolate import interp1d
from datetime import timedelta


class Generator_RT(object):
    """
    Created on Thu Jun 11 13:38:33 2020
    @author: vicxon586
    DATA FROM PERU
    To work in the data of Peru, we build a gap of 3-4 days range so we can get a
    proper increase of the new infections.
    """

    def __init__(self, path="downloads/", name_file="feed_rt.csv", sep=","):
        self.path = path
        dataset = path+name_file
        states = pd.read_csv(dataset, sep=sep,
                             usecols=['Fecha', 'REGION', 'cum_pos_total'],
                             parse_dates=['Fecha'],
                             index_col=['REGION', 'Fecha'],
                             squeeze=True).sort_index()

        states = states.rename_axis(index={'REGION': 'state', 'Fecha': 'date'})
        states = states.rename('positive')
        states.index.get_level_values('date')
        states = states

        # print(states.head())

        # GET GENERAL DATA OF THE COUNTRY
        data_ctry = states.groupby(by='date').sum()
        list_dates = data_ctry.index.to_list()
        tuple_index = [('Peru', date) for date in list_dates]
        final_index = pd.MultiIndex.from_tuples(
            tuple_index, names=['state', 'date'])

        data_ctry = pd.Series(states.groupby(
            by='date').sum().values, index=final_index)
        states = states.append(data_ctry)

        # Fixing data to have difference of two days for the cumulative values
        unq_states = [(state, date) for state, date in states.index]
        unq_states_inverse = unq_states[::-1]
        act_region = unq_states_inverse[0][0]
        act_date = unq_states_inverse[0][1]

        # print(states.head())

        for ind, value in tqdm(enumerate(unq_states_inverse)):
            region = value[0]
            date = value[1]
            if region != act_region:
                #print("Esto es date: {}".format(date-timedelta(days=3)))
                # print(act_region)
                act_region = region
                states = states.drop(
                    [(region, date-timedelta(days=1)), (region, date-timedelta(days=2))])
                act_date = date-timedelta(days=3)
            elif ind + 2 < len(unq_states_inverse):
                if date == act_date and unq_states_inverse[ind+1][0] == act_region and unq_states_inverse[ind+2][0] == act_region:
                    # print(act_date)
                    states = states.drop(
                        [(region, date-timedelta(days=1)), (region, date-timedelta(days=2))])
                    act_date = date-timedelta(days=3)

        # DEFINITION OF HYPERPARAMETERS
        FILTERED_REGION_CODES = ['AS', 'GU', 'PR', 'VI', 'MP', 'MA']

        # Optimal sigma
        k = np.array([20, 40, 55, 90, 150, 200, 1000, 2000, 4000])

        # We create an array for every possible value of Rt
        R_T_MAX = 5
        r_t_range = np.linspace(0, R_T_MAX, R_T_MAX*100+1)

        # Gamma is 1/serial interval
        # https://wwwnc.cdc.gov/eid/article/26/7/20-0282_article
        # https://www.nejm.org/doi/full/10.1056/NEJMoa2001316
        GAMMA = 1/7

        # Map Rt into lambda so we can substitute it into the equation below
        # Note that we have N-1 lambdas because on the first day of an outbreak
        # you do not know what to expect.
        lam = k[:-1] * np.exp(GAMMA * (r_t_range[:, None] - 1))

        sigmas = np.linspace(1/20, 1, 20)

        targets = ~states.index.get_level_values(
            'state').isin(FILTERED_REGION_CODES)
        states_to_process = states.loc[targets]

        results = {}

        # PREPARE CASES AND GET POSTERIORS
        for state_name, cases in tqdm(states_to_process.groupby(level='state')):

            # print(state_name)
            new, smoothed = self.prepare_cases(cases, cutoff=5)

            if len(smoothed) == 0:
                new, smoothed = self.prepare_cases(cases, cutoff=3)

            result = {}

            # Holds all posteriors with every given value of sigma
            result['posteriors'] = []

            # Holds the log likelihood across all k for each value of sigma
            result['log_likelihoods'] = []

            for sigma in sigmas:
                posteriors, log_likelihood = self.get_posteriors(
                    smoothed, GAMMA, lam, sigma=sigma, r_t_range=r_t_range)
                result['posteriors'].append(posteriors)
                result['log_likelihoods'].append(log_likelihood)

            # Store all results keyed off of state name
            results[state_name] = result
            # clear_output(wait=True)

        # print('Done.')

        # CHOOSE OPTIMAL SIGMA

        # Each index of this array holds the total of the log likelihoods for
        # the corresponding index of the sigmas array.
        total_log_likelihoods = np.zeros_like(sigmas)

        # Loop through each state's results and add the log likelihoods to the running total.
        for state_name, result in tqdm(results.items()):
            total_log_likelihoods += result['log_likelihoods']

        # Select the index with the largest log likelihood total
        max_likelihood_index = total_log_likelihoods.argmax()

        # Select the value that has the highest log likelihood
        sigma = sigmas[max_likelihood_index]

        # Plot it
        #fig, ax = plt.subplots()
        #ax.set_title(f"Maximum Likelihood value for $\sigma$ = {sigma:.2f}")
        #ax.plot(sigmas, total_log_likelihoods)
        #ax.axvline(sigma, color='k', linestyle=":")
        # GET FINAL RESULTS OF Rt
        final_results = None

        for state_name, result in tqdm(results.items()):
            # print(state_name)
            posteriors = result['posteriors'][max_likelihood_index]

            hdis_90 = self.highest_density_interval(posteriors, p=.9)

            hdis_50 = self.highest_density_interval(posteriors, p=.5)

            most_likely = posteriors.idxmax().rename('ML')
            result = pd.concat([most_likely, hdis_90, hdis_50], axis=1)
            if final_results is None:
                final_results = result
            else:
                final_results = pd.concat([final_results, result])
            # clear_output(wait=True)

        # print('Done.')

        # DELETE FIRST DATAPOINT
        # Since we now use a uniform prior, the first datapoint is pretty bogus, so just truncating it here
        self.final_results = final_results.groupby(
            'state').apply(lambda x: x.iloc[1:].droplevel(0))

        # EXPORT RESULTS
        # Uncomment the following line if you'd like to export the data
        self.final_results = self.final_results.reset_index()
        self.final_results.rename(columns={"state": "region"}, inplace=True)
        self.final_results.to_csv(
            path+'rt_{}.csv'.format(name_file), index=False)

    @staticmethod
    def get_posteriors(sr, GAMMA, lam, r_t_range, sigma=0.15):
        # (1) Calculate Lambda
        lam = sr[:-1].values * np.exp(GAMMA * (r_t_range[:, None] - 1))

        # (2) Calculate each day's likelihood
        likelihoods = pd.DataFrame(
            data=sps.poisson.pmf(sr[1:].values, lam),
            index=r_t_range,
            columns=sr.index[1:])

        # (3) Create the Gaussian Matrix
        process_matrix = sps.norm(loc=r_t_range,
                                  scale=sigma
                                  ).pdf(r_t_range[:, None])

        # (3a) Normalize all rows to sum to 1
        process_matrix /= process_matrix.sum(axis=0)

        # (4) Calculate the initial prior
        #prior0 = sps.gamma(a=4).pdf(r_t_range)
        prior0 = np.ones_like(r_t_range)/len(r_t_range)
        prior0 /= prior0.sum()

        # Create a DataFrame that will hold our posteriors for each day
        # Insert our prior as the first posterior.
        posteriors = pd.DataFrame(
            index=r_t_range,
            columns=sr.index,
            data={sr.index[0]: prior0}
        )

        # We said we'd keep track of the sum of the log of the probability
        # of the data for maximum likelihood calculation.
        log_likelihood = 0.0

        # (5) Iteratively apply Bayes' rule
        for previous_day, current_day in zip(sr.index[:-1], sr.index[1:]):

            # (5a) Calculate the new prior
            current_prior = process_matrix @ posteriors[previous_day]

            # (5b) Calculate the numerator of Bayes' Rule: P(k|R_t)P(R_t)
            numerator = likelihoods[current_day] * current_prior

            # (5c) Calcluate the denominator of Bayes' Rule P(k)
            denominator = np.sum(numerator)

            # Execute full Bayes' Rule
            posteriors[current_day] = numerator/denominator

            # Add to the running sum of log likelihoods
            log_likelihood += np.log(denominator)

        return posteriors, log_likelihood

    @staticmethod
    def prepare_cases(cases, cutoff=3):
        new_cases = cases.diff()

        smoothed = new_cases.rolling(7,
                                     win_type='gaussian',
                                     min_periods=1,
                                     center=True).mean(std=2).round()

        idx_start = np.searchsorted(smoothed, cutoff)

        smoothed = smoothed.iloc[idx_start:]
        original = new_cases.loc[smoothed.index]

        return original, smoothed

    def highest_density_interval(self, pmf, p=.9, debug=False):
        # If we pass a DataFrame, just call this recursively on the columns
        if(isinstance(pmf, pd.DataFrame)):
            return pd.DataFrame([self.highest_density_interval(pmf[col], p=p) for col in pmf],
                                index=pmf.columns)

        cumsum = np.cumsum(pmf.values)

        # N x N matrix of total probability mass for each low, high
        total_p = cumsum - cumsum[:, None]

        # Return all indices with total_p > p
        lows, highs = (total_p > p).nonzero()

        # Find the smallest range (highest density)
        best = (highs - lows).argmin()

        low = pmf.index[lows[best]]
        high = pmf.index[highs[best]]

        return pd.Series([low, high],
                         index=[f'Low_{p*100:.0f}',
                                f'High_{p*100:.0f}'])
