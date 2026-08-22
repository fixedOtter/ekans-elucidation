import astropy.timeseries as ts

# time in days, color corrected mag, error on mags, and rotation period in hours
def fit_sine(time, mag, error, period):
  # Convert time to hours
  time = time * 24.0

  # Calculate frequency
  freq = 1.0 / (period / 2.0)

  # Create Lomb-Scargle model
  model = ts.LombScargle(time % (period / 2.0), mag, error)

  # Generate fit over one period
  t_fit = np.linspace(0, period, 1000)
  y_fit = model.model(t_fit, freq)

  # Calculate amplitude
  amplitude = np.max(y_fit) - np.min(y_fit)

  return t_fit, y_fit, amplitude