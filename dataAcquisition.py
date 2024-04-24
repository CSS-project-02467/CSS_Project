import pandas as pd
import pywikibot
from joblib import Parallel, delayed
from tqdm import tqdm
import time

def isbot(string):
    return 'bot' in string.lower()

def get_user_contributions(username):
    site = pywikibot.Site('da', 'wikipedia')
    user = pywikibot.User(site, username)
    contributions = user.contributions(total=500)
    articles = [contrib[0].title() for contrib in contributions
                if ':' not in contrib[0].title()]
    return articles

def get_page_contributors(pagename):
    site = pywikibot.Site('da', 'wikipedia')
    page = pywikibot.Page(site, pagename)
    contributors = page.contributors()
    usernames = [user for user in contributors if isbot(user)]
    return {pagename: usernames}

last_df = pd.read_pickle('data/pages_dataframe_140000_to_350000.pkl')
save_point = 350000
start_from = list(last_df['name'])[-1]
print(start_from)

#%%

site = pywikibot.Site('da', 'wikipedia')
all_pages_gen = site.allpages(start=start_from)


def get_page_info(page):
    contributors = list(page.contributors())
    bots = [c for c in contributors if isbot(c)]
    categories = [category.title()[9:] for category in page.categories()]
    return {'name': page.title(), 'bots': bots, 'categories': categories}

page_data = []
counter = 0
save_every = 10000
break_when = 500000

with Parallel(n_jobs=-2, return_as="generator") as parallel:
    data = parallel(delayed(get_page_info)(page)
                    for i, page in enumerate(all_pages_gen))
    pbar = tqdm(total=break_when)
    pbar.update(save_point)
    _ = next(data)
    while True:
        try:
            page = next(data)
            if not page['bots'] == []:
                page_data += [page]
            counter += 1
            if counter % save_every == 0:
                pages_dataframe = pd.DataFrame(page_data)
                pd.to_pickle(pages_dataframe, f'data/pages_dataframe_{save_point}_to_{save_point+counter}.pkl')
            if counter == break_when:
                break
            pbar.update(1)
        except pywikibot.exceptions.ServerError:
            print('Server Error')
            time.sleep(5)
            continue
        except StopIteration:
            print('Done')
            break
    pbar.close()

pages_dataframe = pd.DataFrame(page_data)

site1 = 'https://da.wikipedia.org/w/index.php?title=Kategori:Wikipedia-robotter_-_alle&pageuntil=Synthebot'
site2 = 'https://da.wikipedia.org/w/index.php?title=Kategori:Wikipedia-robotter_-_alle&pagefrom=Synthebot'
r = requests.get(site1)
soup = BeautifulSoup(r.text, 'html.parser')
elements1 = soup.find_all('a')
r = requests.get(site2)
soup = BeautifulSoup(r.text, 'html.parser')
elements2 = soup.find_all('a')
elements = elements1 + elements2
bots = [element.text[7:] for element in elements if element.text[:2] == 'Br']
bots

def only_bots(page):
  return [bot for bot in page['bots'] if bot in bots]

pages_dataframe['bots'] = pages_dataframe.apply(only_bots, axis=1)

pd.to_pickle(pages_dataframe, f'data/pages_dataframe_{save_point}_to_{save_point+counter}.pkl')
