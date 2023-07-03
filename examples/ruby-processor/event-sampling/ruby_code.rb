def init(params)
  @proportion_to_sample = params["proportion_to_sample"].to_f # cast string to float
  @random_num = Random.new
end

def process(event)
  if should_sample(@random_num.rand(100))
    event.put('should_be_sampled', true)
  end
end

def should_sample(random_number)
  return random_number < (100 * @proportion_to_sample)
end