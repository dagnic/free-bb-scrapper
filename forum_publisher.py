import discord
from discord.ext import commands
import sqlite3
import asyncio
from datetime import datetime
import logging
import re

# Configuration du logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
BOT_TOKEN = ""  # Remplacez par votre token Discord
DB_NAME = "forum_data.db"
GUILD_ID = None  # ID du serveur Discord (sera détecté automatiquement)

# Configuration des intents Discord (version minimale)
intents = discord.Intents.none()
intents.guilds = True
intents.guild_messages = True

# Création du bot
bot = commands.Bot(command_prefix='!', intents=intents)

class ForumPublisher:
    def __init__(self, db_path):
        self.db_path = db_path
        self.guild = None
        self.category = None
        
    def get_connection(self):
        return sqlite3.connect(self.db_path)
    
    async def setup_guild(self, guild):
        """Configure le serveur Discord"""
        self.guild = guild
        
        # Créer ou trouver la catégorie "Forum CAASV"
        category_name = "📁 Forum CAASV"
        self.category = discord.utils.get(guild.categories, name=category_name)
        
        if not self.category:
            logger.info(f"Création de la catégorie: {category_name}")
            self.category = await guild.create_category(category_name)
        else:
            logger.info(f"Catégorie trouvée: {category_name}")
    
    def sanitize_channel_name(self, name):
        """Nettoie le nom pour créer un canal Discord valide"""
        # Remplacer les caractères spéciaux par des tirets
        name = re.sub(r'[^\w\s-]', '', name)
        # Remplacer les espaces par des tirets
        name = re.sub(r'\s+', '-', name)
        # Supprimer les tirets multiples
        name = re.sub(r'-+', '-', name)
        # Supprimer les tirets au début et à la fin
        name = name.strip('-')
        # Limiter à 100 caractères et convertir en minuscules
        return name[:100].lower()
    
    def get_forums(self):
        """Récupère tous les forums"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id, group_name, title, description, subjects, replies 
            FROM forums 
            ORDER BY group_name, title
        """)
        forums = cursor.fetchall()
        conn.close()
        return forums
    
    def get_threads_by_forum(self, forum_id):
        """Récupère tous les threads d'un forum"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id, title, author, replies, views, last_date, last_author, url
            FROM threads 
            WHERE forum_id = ? 
            ORDER BY id
        """, (forum_id,))
        threads = cursor.fetchall()
        conn.close()
        return threads
    
    def get_messages_by_thread(self, thread_id):
        """Récupère tous les messages d'un thread"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id, author, content, post_date, post_number 
            FROM messages 
            WHERE thread_id = ? 
            ORDER BY post_number
        """, (thread_id,))
        messages = cursor.fetchall()
        conn.close()
        return messages
    
    async def create_forum_channel(self, forum_data):
        """Crée un canal forum Discord pour un forum"""
        forum_id, group_name, title, description, subjects, replies = forum_data
        
        # Créer le nom du canal
        if group_name and group_name.strip():
            channel_name = f"{group_name}-{title}"
        else:
            channel_name = title
        
        channel_name = self.sanitize_channel_name(channel_name)
        
        # Vérifier si le canal existe déjà
        existing_channel = discord.utils.get(self.guild.channels, name=channel_name)
        if existing_channel:
            logger.info(f"Canal existant trouvé: {channel_name}")
            return existing_channel
        
        # Créer le topic du canal
        topic = f"Forum: {title}"
        if description:
            topic += f" - {description[:500]}"
        if subjects:
            topic += f" | {subjects} sujets, {replies} réponses"
        
        logger.info(f"Création du canal forum: {channel_name}")
        
        try:
            # Créer un canal forum (nouveau type de canal Discord)
            channel = await self.guild.create_forum(
                name=channel_name,
                category=self.category,
                topic=topic[:1024]  # Limite Discord
            )
            return channel
        except Exception as e:
            logger.error(f"Erreur création canal forum {channel_name}: {e}")
            # Fallback: créer un canal texte normal
            channel = await self.guild.create_text_channel(
                name=channel_name,
                category=self.category,
                topic=topic[:1024]
            )
            return channel
    
    async def create_thread_in_forum(self, forum_channel, thread_data, messages):
        """Crée un thread dans un canal forum avec tous les messages"""
        thread_id, title, author, replies, views, last_date, last_author, url = thread_data
        
        if not messages:
            logger.warning(f"Pas de messages pour le thread: {title}")
            return
        
        # Nettoyer le titre pour Discord
        clean_title = title[:100] if title else f"Thread {thread_id}"
        
        # Premier message (contenu du thread)
        first_message = messages[0]
        first_content = first_message[2] if first_message[2] else "Contenu vide"
        
        # Créer le header avec métadonnées
        header = f"**Auteur original:** {first_message[1] or 'Inconnu'}\n" \
                f"**Date:** {first_message[3] or 'Inconnue'}\n\n"
        
        # Calculer l'espace restant pour le contenu (2000 - header - marge de sécurité)
        max_content_length = 2000 - len(header) - 50
        
        # Diviser le contenu en chunks si nécessaire
        content_chunks = []
        if len(first_content) <= max_content_length:
            content_chunks.append(first_content)
        else:
            # Diviser le contenu en morceaux de taille appropriée
            remaining_content = first_content
            while remaining_content:
                if len(remaining_content) <= max_content_length:
                    content_chunks.append(remaining_content)
                    break
                else:
                    # Trouver un bon endroit pour couper (éviter de couper au milieu d'un mot)
                    cut_index = max_content_length
                    while cut_index > max_content_length - 100 and cut_index > 0:
                        if remaining_content[cut_index] in [' ', '\n', '.', '!', '?']:
                            break
                        cut_index -= 1
                    
                    if cut_index <= max_content_length - 100:
                        cut_index = max_content_length
                    
                    content_chunks.append(remaining_content[:cut_index])
                    remaining_content = remaining_content[cut_index:].lstrip()
        
        # Premier chunk avec header
        first_chunk = header + content_chunks[0]
        
        try:
            if hasattr(forum_channel, 'create_thread'):
                # Canal forum moderne
                thread, message = await forum_channel.create_thread(
                    name=clean_title,
                    content=first_chunk
                )
                discord_thread = thread
            else:
                # Canal texte classique
                message = await forum_channel.send(
                    f"# {clean_title}\n\n{first_chunk}"
                )
                
                # Créer un thread à partir du message
                discord_thread = await message.create_thread(name=clean_title)
            
            logger.info(f"Thread créé: {clean_title}")
            
            # Poster les chunks supplémentaires du premier message s'il y en a
            for i, chunk in enumerate(content_chunks[1:], 1):
                await discord_thread.send(f"*(suite {i})*\n{chunk}")
                await asyncio.sleep(0.5)  # Petite pause entre les chunks
            
            # Poster les messages suivants
            for msg in messages[1:]:
                msg_id, author, content, post_date, post_number = msg
                
                if not content or content.strip() == "":
                    continue
                
                # Créer le header pour ce message
                msg_header = f"**{author or 'Inconnu'}** ({post_date or 'Date inconnue'}):\n"
                max_msg_length = 2000 - len(msg_header) - 10
                
                # Diviser le message en chunks si nécessaire
                if len(content) <= max_msg_length:
                    formatted_message = msg_header + content
                    await discord_thread.send(formatted_message)
                else:
                    # Diviser en plusieurs messages
                    remaining_content = content
                    part_num = 1
                    
                    while remaining_content:
                        if len(remaining_content) <= max_msg_length:
                            if part_num == 1:
                                formatted_message = msg_header + remaining_content
                            else:
                                formatted_message = f"*(suite {part_num})*\n{remaining_content}"
                            await discord_thread.send(formatted_message)
                            break
                        else:
                            # Trouver un bon endroit pour couper
                            cut_index = max_msg_length
                            while cut_index > max_msg_length - 100 and cut_index > 0:
                                if remaining_content[cut_index] in [' ', '\n', '.', '!', '?']:
                                    break
                                cut_index -= 1
                            
                            if cut_index <= max_msg_length - 100:
                                cut_index = max_msg_length
                            
                            chunk_content = remaining_content[:cut_index]
                            if part_num == 1:
                                formatted_message = msg_header + chunk_content
                            else:
                                formatted_message = f"*(suite {part_num})*\n{chunk_content}"
                            
                            await discord_thread.send(formatted_message)
                            remaining_content = remaining_content[cut_index:].lstrip()
                            part_num += 1
                
                await asyncio.sleep(1)  # Éviter le rate limiting
            
            logger.info(f"Thread terminé: {clean_title} ({len(messages)} messages)")
            
        except Exception as e:
            logger.error(f"Erreur création thread {clean_title}: {e}")
    
    async def publish_all_forums(self):
        """Publie tous les forums sur Discord"""
        if not self.guild:
            logger.error("Guild non configurée")
            return
        
        forums = self.get_forums()
        logger.info(f"Publication de {len(forums)} forums...")
        
        for forum_data in forums:
            forum_id = forum_data[0]
            forum_title = forum_data[2]
            
            logger.info(f"Traitement du forum: {forum_title}")
            
            # Créer le canal pour ce forum
            forum_channel = await self.create_forum_channel(forum_data)
            
            # Récupérer tous les threads de ce forum
            threads = self.get_threads_by_forum(forum_id)
            logger.info(f"  {len(threads)} threads trouvés")

            for thread_data in reversed(threads):
                thread_id = thread_data[0]
                thread_title = thread_data[1]
                
                logger.info(f"    Traitement du thread: {thread_title[:50]}...")
                
                # Récupérer tous les messages de ce thread
                messages = self.get_messages_by_thread(thread_id)
                
                if messages:
                    await self.create_thread_in_forum(forum_channel, thread_data, messages)

                # Close and archive the thread if possible
                if hasattr(forum_channel, 'threads'):
                    for thread in forum_channel.threads:
                        if thread.name == thread_title[:100]:
                            try:
                                await thread.edit(archived=True, locked=True)
                                logger.info(f"    Thread archivé: {thread_title[:50]}")
                            except Exception as e:
                                logger.error(f"    Erreur archivage thread {thread_title[:50]}: {e}")
                            break
                
                # Pause entre chaque thread
                await asyncio.sleep(2)
            
            logger.info(f"Forum terminé: {forum_title}")
            # Pause entre chaque forum
            await asyncio.sleep(5)
        
        logger.info("Publication terminée!")

# Instance du publisher
publisher = ForumPublisher(DB_NAME)

@bot.event
async def on_ready():
    logger.info(f'{bot.user} s\'est connecté à Discord!')
    print(f'{bot.user} est connecté et prêt!')
    
    # Trouver le serveur (prendre le premier disponible)
    if bot.guilds:
        guild = bot.guilds[0]
        logger.info(f"Serveur trouvé: {guild.name}")
        
        # Configurer le publisher
        await publisher.setup_guild(guild)
        
        # Demander confirmation
        print(f"\n🚀 Prêt à publier le forum sur le serveur: {guild.name}")
        print(f"📁 Catégorie: {publisher.category.name}")
        print("\n⚠️  ATTENTION: Cette opération va créer de nombreux canaux et messages!")
        print("   Assurez-vous que le bot a les permissions nécessaires.")
        
        response = input("\nContinuer? (oui/non): ").lower()
        
        if response in ['oui', 'o', 'yes', 'y']:
            print("\n🔄 Début de la publication...")
            await publisher.publish_all_forums()
            print("\n✅ Publication terminée!")
        else:
            print("\n❌ Publication annulée.")
        
        # Arrêter le bot
        await bot.close()
    else:
        logger.error("Aucun serveur Discord trouvé!")
        await bot.close()

if __name__ == "__main__":
    if BOT_TOKEN == "YOUR_BOT_TOKEN_HERE":
        print("❌ Veuillez configurer votre BOT_TOKEN!")
        print("1. Allez sur https://discord.com/developers/applications")
        print("2. Créez une nouvelle application")
        print("3. Allez dans 'Bot' et créez un bot")
        print("4. Copiez le token et remplacez 'YOUR_BOT_TOKEN_HERE'")
        print("5. Invitez le bot sur votre serveur avec les permissions:")
        print("   - Gérer les canaux")
        print("   - Envoyer des messages")
        print("   - Créer des threads publics")
        print("   - Gérer les messages")
    else:
        try:
            bot.run(BOT_TOKEN)
        except Exception as e:
            logger.error(f"Erreur lors du démarrage du bot: {e}")
