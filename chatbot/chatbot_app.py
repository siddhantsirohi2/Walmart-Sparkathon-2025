# chatbot/chatbot_app.py
import json
import os
import threading
import time

# LangChain Imports
from langchain_openai import ChatOpenAI # For OpenAI's Chat models
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain.agents import create_tool_calling_agent, AgentExecutor
from langchain.tools import tool # A decorator to define tools

# Kafka Consumer Import (for potential KB updates)
from kafka import KafkaConsumer

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


# --- LangChain Tools ---

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


# --- Main Chatbot Logic ---

def setup_chatbot_agent():
    """
    Sets up the LangChain agent with the LLM and custom tools.
    """
    # Initialize the LLM. Langchain's ChatOpenAI by default reads OPENAI_API_KEY
    # from environment variables or a .env file.
    try:
        llm = ChatOpenAI(model="gpt-3.5-turbo", temperature=0.4)
        print("OpenAI LLM initialized successfully.")
    except Exception as e:
        print(f"Error initializing OpenAI LLM: {e}")
        print("Please ensure your OPENAI_API_KEY environment variable is set correctly.")
        print("You can set it in your terminal like: $env:OPENAI_API_KEY='sk-YOUR_KEY' (PowerShell)")
        print("Or: export OPENAI_API_KEY='sk-YOUR_KEY' (Bash/WSL)")
        exit(1)

    # Define the tools available to the LLM
    tools = [get_apriori_rules_for_items]

    # Define the prompt for the LLM
    system_message = (
        "You are an intelligent assistant specializing in market basket analysis for a grocery store. "
        "Your goal is to explain why certain grocery items are frequently bought together "
        "based on provided association rules. "
        "If a user asks about relationships between items, use the 'get_apriori_rules_for_items' tool "
        "to retrieve relevant rules from the knowledge base. "
        "Extract the item names accurately from the user's query for the tool. "
        "Then, explain the relationship clearly, mentioning the confidence and support values for the rule(s). "
        "If no rules are found for the requested items, clearly state that. "
        "For the 'Beer' and 'Diapers' relationship, you can also mention the common anecdote about new parents "
        "making late-night purchases."
        "\n\nContextual Rule Explanation Examples:"
        "\n- For 'Milk and Bread': Explain that they are staples often bought together, essential for daily needs."
        "\n- For 'Beer and Diapers': Explain the common anecdotal theory about new parents."
    )

    prompt = ChatPromptTemplate.from_messages([
        ("system", system_message),
        MessagesPlaceholder(variable_name="chat_history", optional=True),
        ("human", "{input}"),
        MessagesPlaceholder(variable_name="agent_scratchpad"),
    ])

    # Create the agent executor
    agent = create_tool_calling_agent(llm, tools, prompt)
    agent_executor = AgentExecutor(agent=agent, tools=tools, verbose=True)

    return agent_executor


if __name__ == '__main__':
    # Load environment variables from .env file (if it exists)
    from dotenv import load_dotenv
    load_dotenv()

    # Load the initial knowledge base from the JSON file
    # This must happen before the chatbot starts receiving queries
    load_apriori_knowledge_base()

    # Start Kafka listener for KB updates in a separate thread
    kb_listener_thread = threading.Thread(target=listen_for_kb_updates, daemon=True)
    kb_listener_thread.start()

    print("Setting up LangChain chatbot agent...")
    agent_chain = setup_chatbot_agent()
    print("Chatbot is ready! Type 'exit' to quit.")

    # Start the interactive chat loop
    while True:
        try:
            user_input = input("\nYour query: ")
            if user_input.lower() == 'exit':
                break

            # Invoke the LangChain agent with the user's query
            response = agent_chain.invoke({"input": user_input})

            # Print the LLM's final answer
            print(f"Chatbot: {response['output']}")

        except KeyboardInterrupt:
            print("\nChatbot shutting down.")
            break
        except Exception as e:
            print(f"An error occurred: {e}")
            print("Please check your API key, Kafka connection, and knowledge base file path.")
            continue

    print("Chatbot application ended.")