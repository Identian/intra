export const currEnv = 'dev';

export const envs = {
  'dev0': 'dev',
  'dev': 'dev',
  'qa': 'qa',
  'prod': 'p'
}

export const url = {
  'dev0': 'https://xcrsy02d5f.execute-api.us-east-1.amazonaws.com/',
  'dev': 'https://d16gaht4alt8xf.cloudfront.net',
  'qa': 'https://qa-intradiapp.precia.co',
  'prod': 'https://intradiapp.precia.co'
}

export const tenant = {
  'dev0': 'caa1dfbf-34d5-4061-9cdf-0ceaa516bf03',
  'dev': 'caa1dfbf-34d5-4061-9cdf-0ceaa516bf03',
  'qa': '1f5a45f4-afdd-40b4-8285-ba955d4fafaf',
  'prod': '1f5a45f4-afdd-40b4-8285-ba955d4fafaf'
}

export const clientId = {
  'dev0': 'd278b34e-8140-4710-a4b0-d0e5a953cceb',
  'dev': 'd278b34e-8140-4710-a4b0-d0e5a953cceb',
  'qa': '976c5265-1fde-4d2b-b8be-34f9b29abcd4',
  'prod': 'cc9cd684-7cf5-4c90-8a4d-e518e0968e63'
}

export const urlToUse = url[currEnv];
export const tenantToUse = tenant[currEnv];
export const clientIdToUse = clientId[currEnv];
export const envToUse = envs[currEnv];


export const environment = {
  production: false,
  api: url[currEnv] + '/' + envs[currEnv]
};
