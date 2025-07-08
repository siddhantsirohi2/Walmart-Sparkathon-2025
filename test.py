# chatbot/chatbot_app.py
import json
import os
import threading
import time
# LangChain Imports
from langchain_groq import ChatGroq
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain.agents import create_tool_calling_agent, AgentExecutor
from langchain.tools import tool # A decorator to define tools

# Kafka Consumer Import (for potential KB updates)
from kafka import KafkaConsumer
from collections import deque


import dotenv
dotenv.load_dotenv()
print("🔑 GROQ_API_KEY =", os.getenv("GROQ_API_KEY"))
# --- Configuration ---
# Path to the Apriori knowledge base JSON file
# IMPORTANT: Adjust this path based on where you run chatbot_app.py
# If you run `python chatbot_app.py` from `c:\CODING\Walmart Sparkathon\chatbot\`
# The knowledge base file is in `c:\CODING\Walmart Sparkathon\consumer\`
KNOWLEDGE_BASE_FILE = os.path.join(os.path.dirname(__file__), '..', 'consumer', 'apriori_knowledge_base.json')


# Kafka settings for the KB update listener
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092'] # Connects to Kafka on your host mapped port
KAFKA_KB_UPDATE_TOPIC = 'apriori-rules-output' # Topic to listen for new rules

# --- Global In-memory Knowledge Base ---
# This will hold the parsed Apriori rules for quick lookup by the LLM tool
global_apriori_knowledge_base = []
kb_lock = threading.Lock() # To prevent race conditions during updates

# --- Global In-memory SHAP Store ---
global_shap_values_store = {}
shap_lock = threading.Lock()
KAFKA_SHAP_TOPIC = 'shap_values'

# --- Global In-memory Stock Status Store ---
global_stock_status_store = {}
stock_status_lock = threading.Lock()
KAFKA_STATUS_TOPIC = 'current_status'

# --- Product Mapping (Product Name <-> ID) ---
product_name_to_id = {
    "Jam": 1,
    "Eggs": 2,
    "Cheese": 3,
    "Sugar": 4,
    "Juice": 5,
    "Milk": 6,
    "Yogurt": 7,
    "Beer": 8,
    "Butter": 9,
    "Diapers": 10,
    "Bread": 11
}
product_id_to_name = {v: k for k, v in product_name_to_id.items()}

def load_apriori_knowledge_base():
    """
    Loads/reloads the Apriori rules from the JSON file into memory.
    """
    global global_apriori_knowledge_base
    try:
        # Use the adjusted kb_path directly
        kb_path = KNOWLEDGE_BASE_FILE # Use the already correctly set KNOWLEDGE_BASE_FILE

        if not os.path.exists(kb_path):
            print(f"Knowledge base file not found at {kb_path}. Please run the Apriori producer and consumer first to generate it.")
            # Do not return immediately, allow the program to continue with an empty KB if it doesn't exist yet
            with kb_lock:
                global_apriori_knowledge_base = []
            return

        with open(kb_path, 'r') as f:
            data = json.load(f)
            with kb_lock:
                global_apriori_knowledge_base = data
            print(f"Knowledge base reloaded. {len(global_apriori_knowledge_base)} rules loaded from {kb_path}.")
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"Error loading knowledge base: {e}. Starting with an empty KB.")
        with kb_lock:
            global_apriori_knowledge_base = []


def listen_for_kb_updates():
    """
    Kafka consumer to listen for updates to the knowledge base.
    When a new rule is detected (or any message on this topic), it triggers a reload.
    Note: For a production system, you might want a more granular update mechanism
    or only specific 'update' messages. Here, any message on the topic triggers reload.
    """
    consumer = None
    try:
        consumer = KafkaConsumer(
            KAFKA_KB_UPDATE_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            auto_offset_reset='latest', # Only consume new messages from now on
            enable_auto_commit=True,
            group_id='chatbot-kb-listener-group',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        print(f"Chatbot KB listener started. Listening on topic: {KAFKA_KB_UPDATE_TOPIC}")

        for message in consumer:
            print(f"Detected new rule via Kafka: {message.value}. Reloading knowledge base...")
            load_apriori_knowledge_base() # Reload the entire KB
    except Exception as e:
        print(f"Error in Kafka KB listener: {e}")
        print("Ensure Kafka is running and accessible at localhost:9092.")
    finally:
        if consumer:
            consumer.close()
            print("Kafka KB listener closed.")

def listen_for_shap_updates():
    """
    Kafka consumer to listen for SHAP values.
    Stores them in global_shap_values_store using product_id as key.
    """
    consumer = None
    try:
        consumer = KafkaConsumer(
            KAFKA_SHAP_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id='chatbot-shap-listener-group',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        print(f"Chatbot SHAP listener started. Listening on topic: {KAFKA_SHAP_TOPIC}")

        for message in consumer:
            shap_data = message.value
            product_id = shap_data.get('product_id')
            if product_id is not None:
                with shap_lock:
                    global_shap_values_store[product_id] = shap_data
                print(f"SHAP values stored for product_id: {product_id}")

    except Exception as e:
        print(f"Error in Kafka SHAP listener: {e}")
    finally:
        if consumer:
            consumer.close()
            print("Kafka SHAP listener closed.")

def listen_for_stock_status_updates():
    """
    Kafka consumer to listen for stock status updates.
    Stores them in global_stock_status_store using (store_id, product_id) as key.
    """
    consumer = None
    try:
        consumer = KafkaConsumer(
            KAFKA_STATUS_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id='chatbot-stock-listener-group',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        print(f"Chatbot stock status listener started. Listening on topic: {KAFKA_STATUS_TOPIC}")

        for message in consumer:
            status_data = message.value
            key = (status_data.get('store_id'), status_data.get('product_id'))
            if all(key):
                with stock_status_lock:
                    global_stock_status_store[key] = status_data
                # print(f"✅ Stock status stored for Store {key[0]}, Product {key[1]}")

    except Exception as e:
        print(f"❌ Error in Kafka stock status listener: {e}")
    finally:
        if consumer:
            consumer.close()
            print("Kafka stock status listener closed.")


# --- LangChain Tools ---
@tool
def get_product_name_or_id(query: str) -> str:
    """
    Converts a product name to product ID or vice versa.
    Example: 'Milk' → 6, or '6' → 'Milk'
    """
    query = query.strip().capitalize()
    
    if query.isdigit():
        product_id = int(query)
        return product_id_to_name.get(product_id, f"No product found for ID {product_id}")
    else:
        return str(product_name_to_id.get(query, f"No product ID found for product name '{query}'"))

@tool
def get_apriori_rules_for_items(item_names: str) -> str: # item_names as string for simpler LLM tool input
    """
    Searches the Apriori knowledge base for association rules involving the given comma-separated items.
    Returns a string of relevant rules.
    Example: get_apriori_rules_for_items("Beer, Diapers")
    """
    # Parse the input string into a list of items
    items_list = [item.strip() for item in item_names.split(',') if item.strip()]
    if not items_list:
        return "No items provided for search."

    item_names_set = frozenset(items_list)
    relevant_rules = []

    with kb_lock: # Ensure thread-safe access to the global KB
        for rule in global_apriori_knowledge_base:
            antecedent_set = frozenset(rule['antecedent'])
            consequent_set = frozenset(rule['consequent'])

            # Check if any of the queried items are in the antecedent OR consequent of a rule
            if any(item in item_names_set for item in antecedent_set) or \
               any(item in item_names_set for item in consequent_set):
                relevant_rules.append(
                    f"Rule: {list(antecedent_set)} => {list(consequent_set)} "
                    f"(Conf: {rule['confidence']:.2f}, Supp: {rule['support']:.2f})"
                )

    if relevant_rules:
        return "Found the following rules:\n" + "\n".join(relevant_rules)
    return "No specific Apriori rules found for these items in the knowledge base."

@tool
def get_shap_explanation_for_product(product_id: str) -> str:
    """
    Returns the SHAP value explanation for the given product_id.
    Includes feature contributions and base value.
    """
    with shap_lock:
        shap_data = global_shap_values_store.get(product_id)

    if not shap_data:
        return f"No SHAP explanation found for product_id: {product_id}"

    explanation = f"Prediction Explanation for Product ID {product_id}:\n"
    explanation += f"Base Value: {shap_data['base_value']:.2f}\n"
    explanation += f"Prediction: {shap_data['prediction']:.2f}\n"
    explanation += "Feature Contributions:\n"
    for feature, value in shap_data['features'].items():
        explanation += f"  {feature}: {value:.2f}\n"

    explanation += "\nGlobal Model Coefficients (Feature Importance Across All Products):\n"
    for feature, coef in shap_data.get('coefficients', {}).items():
        explanation += f"  {feature}: {coef:.2f}\n"

    return explanation

@tool
def get_stock_status(store_product_key: str) -> str:
    """
    Returns the stock status for a given (store_id, product_id).
    Provide input as 'store_id, product_id' → e.g., '1, 5'
    """
    try:
        store_id, product_id = map(int, store_product_key.split(','))
    except:
        return "Invalid input. Provide input as 'store_id, product_id' (e.g., '1, 5')"

    with stock_status_lock:
        status = global_stock_status_store.get((store_id, product_id))

    if not status:
        return f"No stock status available for Store {store_id}, Product {product_id}."

    return (
        f"Stock Status for Store {store_id}, Product {product_id}:\n"
        f"  Status: {status['status']}"
    )


# --- Main Chatbot Logic ---

def setup_chatbot_agent():
    """
    Sets up the LangChain agent with the LLM and custom tools.
    """
    # Initialize the LLM. Langchain's ChatOpenAI by default reads OPENAI_API_KEY
    # from environment variables or a .env file.

    try:
        llm = ChatGroq(
            groq_api_key=os.getenv("GROQ_API_KEY"),  # You can load from env as well
            model_name="llama3-70b-8192"     # Example model name from Groq
        )
        # llm = ChatOpenAI(model="gpt-3.5-turbo", temperature=0.4)
        # print("OpenAI LLM initialized successfully.")
    except Exception as e:
        print(f"Error initializing OpenAI LLM: {e}")
        print("Please ensure your OPENAI_API_KEY environment variable is set correctly.")
        print("You can set it in your terminal like: $env:OPENAI_API_KEY='sk-YOUR_KEY' (PowerShell)")
        print("Or: export OPENAI_API_KEY='sk-YOUR_KEY' (Bash/WSL)")
        exit(1)

    # Define the tools available to the LLM
    tools = [get_apriori_rules_for_items, get_shap_explanation_for_product, get_stock_status, get_product_name_or_id]
    # Add dictionary of mapping here by GPT and send it as context to the LLM

    # Define the prompt for the LLM
    system_message = (
    "You are an intelligent assistant specializing in inventory optimization and sales prediction explainability.\n"
    "You have access to the following tools:\n"
    "1. get_apriori_rules_for_items - for checking association rules between products.\n"
    "2. get_shap_explanation_for_product - for SHAP-based sales prediction explanations.\n"
    "3. get_stock_status - for checking understock/overstock status in a store.\n"
    "4. get_product_name_or_id - for converting between product names and IDs.\n\n"
    "Use these tools smartly based on user queries:\n"
    "- If the user asks about which items are frequently bought together, use get_apriori_rules_for_items.\n"
    "- If the user asks 'why was this predicted' or about model reasoning for a product, use get_shap_explanation_for_product.\n"
    "- If the user asks about current stock status for a product at a specific store, use get_stock_status.\n"
    "- If the user provides product name and you need an ID (or vice versa), use get_product_name_or_id.\n"
    "Be concise, helpful, and make sure to include reasoning where relevant."
    )

    prompt = ChatPromptTemplate.from_messages([
        ("system", system_message),
        MessagesPlaceholder(variable_name="chat_history", optional=True),
        ("human", "{input}"),
        MessagesPlaceholder(variable_name="agent_scratchpad"),
    ])

    # Create the agent executor
    agent = create_tool_calling_agent(llm, tools, prompt)
    agent_executor = AgentExecutor(agent=agent, tools=tools, verbose=False)

    return agent_executor


if __name__ == '__main__':
    # Load environment variables from .env file (if it exists)

    # Load the initial knowledge base from the JSON file
    # This must happen before the chatbot starts receiving queries
    load_apriori_knowledge_base()

    # Start Kafka listener for KB updates in a separate thread
    kb_listener_thread = threading.Thread(target=listen_for_kb_updates, daemon=True)
    kb_listener_thread.start()
    shap_listener_thread = threading.Thread(target=listen_for_shap_updates, daemon=True)
    shap_listener_thread.start()
    stock_listener_thread = threading.Thread(target=listen_for_stock_status_updates, daemon=True)
    stock_listener_thread.start()

    print("Setting up LangChain chatbot agent...")
    agent_chain = setup_chatbot_agent()
    print("Chatbot is ready! Type 'exit' to quit.")
    chat_history = deque(maxlen=10)  # stores last 10 exchanges

    # Start the interactive chat loop
    while True:
        try:
            user_input = input("\nYour query: ")
            if user_input.lower() == 'exit':
                break
            
            # Invoke the LangChain agent with the user's query
            response = agent_chain.invoke({"input": user_input,
                                           "chat_history": list(chat_history)
                                        })

            # Print the LLM's final answer
            print(f"Chatbot: {response['output']}")
            chat_history.append(("human", user_input))
            chat_history.append(("ai", response['output']))
            
        except KeyboardInterrupt:
            print("\nChatbot shutting down.")
            break
        except Exception as e:
            print(f"An error occurred: {e}")
            print("Please check your API key, Kafka connection, and knowledge base file path.")
            continue

    print("Chatbot application ended.")