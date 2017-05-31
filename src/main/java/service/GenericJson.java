package service;

import com.fasterxml.jackson.annotation.JsonProperty;

public class GenericJson 
{
	@JsonProperty("type")
    public String type;
	
    @JsonProperty("value")
    public String value;
}
