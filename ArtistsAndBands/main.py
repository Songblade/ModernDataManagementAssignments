import csv
import redis
import pymongo
# import time


def set_up_redis() -> redis.Redis:
    redis_db = redis.Redis(host='localhost', port=6379, decode_responses=True)
    redis_db.select(4)  # using partition 4 for the assignment
    redis_db.flushdb()  # so that we don't get confused by previous assignments
    return redis_db
    # My redis schema has bands_of:<artist_name> connecting to the set of bands
    # It has artists_in:<band_name> connecting to the set of artists
    # It has <artist_name>:roles_in:<band_name> connecting to a set of the artist's roles in the band


def convert_to_redis(redis_db: redis.Redis):
    # In real life, with a bigger dataset, this wouldn't work
    # But with our small dataset, this is faster
    bands_of_artist = {}
    artists_in_band = {}
    artist_roles_in_band = {}

    with open("group_membership.tsv", mode='rt', newline='', encoding='utf8') as band_file:

        tsv_file = csv.reader(band_file, delimiter="\t")
        next(tsv_file)  # skip the header row
        num_lines = 0
        num_useful_lines = 0

        for num_lines, line in enumerate(tsv_file):
            # First, we extract the important properties
            artist = line[2]
            band = line[3]
            roles = line[4].rsplit(',')

            if len(artist) and len(band):  # if both are present
                # print(f'{artist} at {band} doing {roles}')
                if artist not in bands_of_artist:
                    bands_of_artist[artist] = set()
                bands_of_artist[artist].add(band)

                if band not in artists_in_band:
                    artists_in_band[band] = set()
                artists_in_band[band].add(artist)

                if roles != ['']:  # if it's not empty
                    if (artist, band) not in artist_roles_in_band:
                        artist_roles_in_band[(artist, band)] = set()
                    artist_roles_in_band[(artist, band)].update(roles)

                num_useful_lines += 1

            # print(f'{artist} at {band} doing {roles}')
            if num_lines % 10000 == 0:
                print(f'So far: processed {num_lines} lines')

        num_lines += 1  # because the first line, line 0, isn't counted
        print(f"Total lines processed: {num_lines}")
        print(f'Useful lines processed: {num_useful_lines}')
        # print(artists_in_band.get('The Beatles'))
        # print(bands_of_artist.get('Paul McCartney'))
        # print(artist_roles_in_band.get(('Cale Harper', 'Horses About Men')))

        # I originally added each thing to Redis when I extracted it from the TSV file
        # But I found this to be an order of magnitude faster
        # And then I got it another order of magnitude faster by using a single pipeline for much less I/O cost
        pipe = redis_db.pipeline(transaction=False)
        pipe.sadd('all_bands', *artists_in_band.keys())
        for artist in bands_of_artist:
            pipe.sadd(f'bands_of:{artist}', *bands_of_artist[artist])
        for band in artists_in_band:
            pipe.sadd(f'artists_in:{band}', *artists_in_band[band])
        for (artist, band) in artist_roles_in_band:
            pipe.sadd(f'{artist}:roles_in:{band}', *artist_roles_in_band.get((artist, band)))
        pipe.execute()


# Mongo schema:
# {
#   name: String
#   artists: [{
#       name: String
#       roles: [String]  (empty array if missing)
#   }]
# }
def convert_to_mongo(redis_db: redis.Redis):
    # Okay, now I need to bring Redis into Mongo
    # I can ignore the lookup of bands by artist
    # For each band, I just transfer the list of artists directly
    # But, for each artist, I need to add their roles, if they have any

    mongo_insert = []

    # first, we get all the bands
    all_bands = redis_db.smembers('all_bands')

    band_pipe = redis_db.pipeline()
    for band in all_bands:
        band_pipe.smembers(f'artists_in:{band}')
    band_artists = band_pipe.execute()

    # print(f'After getting redis bands: {time.time() - elapsed}')

    # I found that it was too slow querying the roles for every role
    # So instead, I first found all the band-artist pairs with roles, and only queried for them

    band_doc_samples = {band: {artist: [] for artist in artists} for band, artists in zip(all_bands, band_artists)}

    artist_role_keys = redis_db.keys('*:roles_in:*')
    for artist_role_key in artist_role_keys:
        end_of_artist = artist_role_key.find(':roles_in:')
        artist = artist_role_key[:end_of_artist]
        band = artist_role_key[end_of_artist + len(':roles_in:'):]
        roles = list(redis_db.smembers(artist_role_key))
        band_doc_samples[band][artist] = roles

    # Now we translate into Mongo style
    for band in band_doc_samples:
        mongo_insert.append({'name': band, 'artists': [{'name': artist, 'roles': band_doc_samples[band][artist]}
                                                       for artist in band_doc_samples[band]]})

    # print(f'After preparing for Mongo: {time.time() - elapsed}')

    band_collection = pymongo.MongoClient().artists_and_bands.band_collection
    band_collection.drop()
    band_collection.insert_many(mongo_insert, ordered=False)


if __name__ == '__main__':
    redis_42 = set_up_redis()
    # print(f'After setting up redis: {time.time() - elapsed}')
    convert_to_redis(redis_42)
    # print(f'After submitting to redis: {time.time() - elapsed}')
    convert_to_mongo(redis_42)
    # print(f'After submitting to Mongo: {time.time() - elapsed}')
